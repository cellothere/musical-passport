require("dotenv").config();
const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");
const path = require("path");
const session = require("express-session");
const querystring = require("querystring");
const { createSign } = require("crypto");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.set('trust proxy', 1); // Required for Railway/Heroku reverse proxy
const PORT = process.env.PORT || 3000;

// Spotify OAuth config
const SPOTIFY_CLIENT_ID = process.env.SPOTIFY_CLIENT_ID;
const SPOTIFY_CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET;
const SPOTIFY_REDIRECT_URI = process.env.SPOTIFY_REDIRECT_URI;

// Cache for client credentials token
let clientAccessToken = null;
let tokenExpiry = null;

// Get client credentials access token for public API access
async function getClientAccessToken() {
  // Return cached token if still valid
  if (clientAccessToken && tokenExpiry && Date.now() < tokenExpiry) {
    return clientAccessToken;
  }

  try {
    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization:
          "Basic " +
          Buffer.from(SPOTIFY_CLIENT_ID + ":" + SPOTIFY_CLIENT_SECRET).toString("base64"),
      },
      body: "grant_type=client_credentials",
    });

    const data = await response.json();

    if (data.access_token) {
      clientAccessToken = data.access_token;
      tokenExpiry = Date.now() + (data.expires_in - 60) * 1000; // Refresh 1 min before expiry
      console.log("Got new client credentials token");
      return clientAccessToken;
    }

    throw new Error("Failed to get client credentials token");
  } catch (err) {
    console.error("Error getting client access token:", err);
    return null;
  }
}

// ── Apple Music developer token (ES256 JWT) ──────────────
let appleMusicToken = null;
let appleMusicTokenExpiry = null;

function generateAppleMusicToken() {
  if (appleMusicToken && appleMusicTokenExpiry && Date.now() < appleMusicTokenExpiry) {
    return appleMusicToken;
  }
  const teamId = process.env.APPLE_TEAM_ID;
  const keyId  = process.env.APPLE_KEY_ID;
  const rawKey = process.env.APPLE_PRIVATE_KEY;
  if (!teamId || !keyId || !rawKey) return null;

  try {
    const privateKey = rawKey.replace(/\\n/g, "\n");
    const now    = Math.floor(Date.now() / 1000);
    const expiry = now + 15552000; // 180 days
    const header  = Buffer.from(JSON.stringify({ alg: "ES256", kid: keyId })).toString("base64url");
    const payload = Buffer.from(JSON.stringify({ iss: teamId, iat: now, exp: expiry })).toString("base64url");
    const input   = `${header}.${payload}`;
    const sign    = createSign("SHA256");
    sign.update(input);
    const sig = sign.sign({ key: privateKey, dsaEncoding: "ieee-p1363" }, "base64url");
    appleMusicToken       = `${input}.${sig}`;
    appleMusicTokenExpiry = (expiry - 3600) * 1000; // refresh 1h before expiry
    return appleMusicToken;
  } catch (err) {
    console.error("Apple Music token generation failed:", err.message);
    return null;
  }
}

// ── Supabase client ───────────────────────────────────────
const supabase = process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)
  : null;

// ── Spotify user ID resolution (with 5-min in-memory cache) ──
const userIdCache = new Map(); // token -> { spotifyId, expiresAt }

async function resolveSpotifyUser(authHeader) {
  if (!authHeader?.startsWith("Bearer ")) return null;
  const token = authHeader.slice(7);
  const cached = userIdCache.get(token);
  if (cached && Date.now() < cached.expiresAt) return cached.spotifyId;
  try {
    const r = await fetch("https://api.spotify.com/v1/me", {
      headers: { Authorization: "Bearer " + token },
    });
    if (!r.ok) return null;
    const data = await r.json();
    userIdCache.set(token, { spotifyId: data.id, expiresAt: Date.now() + 5 * 60 * 1000 });
    return data.id;
  } catch {
    return null;
  }
}

// ── Spotify enrichment helpers ────────────────────────────

async function spotifyGet(path, accessToken) {
  const r = await fetch(`https://api.spotify.com/v1${path}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!r.ok) return null;
  return r.json();
}

// Fetch top artists + top tracks across all 3 time ranges
async function enrichFromSpotify(accessToken) {
  const TIME_WEIGHTS = { short_term: 3, medium_term: 2, long_term: 1 };
  const timeRanges = ["short_term", "medium_term", "long_term"];

  // Parallel fetch: top artists × 3 ranges + top tracks × 3 ranges
  const [artistResults, trackResults] = await Promise.all([
    Promise.all(timeRanges.map(tr =>
      spotifyGet(`/me/top/artists?time_range=${tr}&limit=50`, accessToken).catch(() => null)
    )),
    Promise.all(timeRanges.map(tr =>
      spotifyGet(`/me/top/tracks?time_range=${tr}&limit=50`, accessToken).catch(() => null)
    )),
  ]);

  // Build weighted artist map: artistId → { artist, weight }
  const artistMap = new Map();
  for (let i = 0; i < timeRanges.length; i++) {
    const items = artistResults[i]?.items || [];
    const w = TIME_WEIGHTS[timeRanges[i]];
    for (const artist of items) {
      if (artistMap.has(artist.id)) {
        artistMap.get(artist.id).weight += w;
      } else {
        artistMap.set(artist.id, { artist, weight: w });
      }
    }
  }

  // Collect track release years (for era calculation) + artist IDs from tracks
  const releaseYears = [];
  const trackArtistIds = new Set();
  for (let i = 0; i < timeRanges.length; i++) {
    const items = trackResults[i]?.items || [];
    const w = TIME_WEIGHTS[timeRanges[i]];
    for (const track of items) {
      const year = track.album?.release_date ? parseInt(track.album.release_date.slice(0, 4)) : null;
      if (year && year > 1900) releaseYears.push({ year, weight: w });
      for (const a of track.artists) {
        if (!artistMap.has(a.id)) trackArtistIds.add(a.id);
      }
    }
  }

  // Batch-fetch genres for track-only artists so Claude has more context
  const unknownIds = [...trackArtistIds].slice(0, 150);
  for (let i = 0; i < unknownIds.length; i += 50) {
    const chunk = unknownIds.slice(i, i + 50);
    const data = await spotifyGet(`/artists?ids=${chunk.join(",")}`, accessToken).catch(() => null);
    for (const artist of data?.artists || []) {
      if (artist && !artistMap.has(artist.id)) {
        artistMap.set(artist.id, { artist, weight: 0.5 });
      }
    }
  }

  // ── Era distribution from actual track release dates ──────
  // This is the one thing Spotify can calculate accurately that Claude cannot.
  const eraScores = {};
  let totalEraWeight = 0;
  for (const { year, weight } of releaseYears) {
    const decade = `${Math.floor(year / 10) * 10}s`;
    eraScores[decade] = (eraScores[decade] || 0) + weight;
    totalEraWeight += weight;
  }
  const eraRaw = Object.entries(eraScores)
    .map(([decade, score]) => ({ decade, pct: Math.round((score / totalEraWeight) * 100) }))
    .sort((a, b) => b.pct - a.pct)
    .slice(0, 5);

  // Artists sorted by weight, with Spotify genre tags as hints for Claude
  const topArtistsEnriched = [...artistMap.values()]
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 50)
    .map(({ artist, weight }) => ({
      name: artist.name,
      genres: artist.genres.slice(0, 4),
      weight,
    }));

  return { topArtistsEnriched, eraRaw, totalArtists: artistMap.size };
}

const formatFavorite = (row) => ({
  id: row.id,
  type: row.type,
  country: row.country,
  decade: row.decade ?? undefined,
  savedAt: new Date(row.saved_at).getTime(),
  data: row.data,
});

function makeCacheKey(parts) {
  return parts.filter(Boolean).join("_").toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // ó→o, ú→u, ø→o, etc.
    .replace(/\s+/g, "-").replace(/[^a-z0-9_-]/g, "");
}

async function getCached(cacheKey) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from("recommendation_cache")
    .select("*")
    .eq("cache_key", cacheKey)
    .single();
  if (error || !data) return null;
  supabase.from("recommendation_cache")
    .update({ hit_count: data.hit_count + 1, last_accessed_at: new Date().toISOString() })
    .eq("cache_key", cacheKey)
    .then(() => {});
  return data;
}

async function storeCache(cacheKey, endpoint, result, artistPool = null) {
  if (!supabase) return;
  await supabase.from("recommendation_cache").upsert(
    { cache_key: cacheKey, endpoint, result, artist_pool: artistPool },
    { onConflict: "cache_key" }
  );
}

function pickRandom(arr, n) {
  return [...arr].sort(() => Math.random() - 0.5).slice(0, n);
}

const REGION_BY_CODE = {
  // Africa
  ZA:"Africa",NG:"Africa",GH:"Africa",SN:"Africa",ET:"Africa",CM:"Africa",KE:"Africa",
  EG:"Africa",MA:"Africa",TZ:"Africa",CI:"Africa",AO:"Africa",MZ:"Africa",ZW:"Africa",
  UG:"Africa",RW:"Africa",ZM:"Africa",TN:"Africa",LY:"Africa",SD:"Africa",GN:"Africa",
  BF:"Africa",BJ:"Africa",TG:"Africa",SL:"Africa",LR:"Africa",NA:"Africa",BW:"Africa",
  MW:"Africa",MG:"Africa",MU:"Africa",CV:"Africa",
  // Asia
  JP:"Asia",KR:"Asia",IN:"Asia",CN:"Asia",ID:"Asia",TH:"Asia",VN:"Asia",PH:"Asia",
  PK:"Asia",BD:"Asia",TW:"Asia",MN:"Asia",MM:"Asia",KH:"Asia",LA:"Asia",MY:"Asia",
  SG:"Asia",LK:"Asia",NP:"Asia",AF:"Asia",KZ:"Asia",UZ:"Asia",TJ:"Asia",KG:"Asia",
  TM:"Asia",HK:"Asia",
  // Europe
  FR:"Europe",DE:"Europe",SE:"Europe",NO:"Europe",PT:"Europe",ES:"Europe",IT:"Europe",
  GR:"Europe",PL:"Europe",IS:"Europe",FI:"Europe",IE:"Europe",NL:"Europe",RO:"Europe",
  RS:"Europe",UA:"Europe",HU:"Europe",CZ:"Europe",TR:"Europe",BE:"Europe",CH:"Europe",
  AT:"Europe",DK:"Europe",GB:"Europe",HR:"Europe",BG:"Europe",SK:"Europe",SI:"Europe",
  LT:"Europe",LV:"Europe",EE:"Europe",AL:"Europe",MK:"Europe",BA:"Europe",ME:"Europe",
  LU:"Europe",MT:"Europe",CY:"Europe",
  // Latin America
  BR:"Latin America",AR:"Latin America",CO:"Latin America",CU:"Latin America",
  MX:"Latin America",CL:"Latin America",PE:"Latin America",JM:"Latin America",
  VE:"Latin America",BO:"Latin America",EC:"Latin America",PA:"Latin America",
  UY:"Latin America",PY:"Latin America",CR:"Latin America",DO:"Latin America",
  PR:"Latin America",GT:"Latin America",HN:"Latin America",SV:"Latin America",
  NI:"Latin America",BZ:"Latin America",GY:"Latin America",SR:"Latin America",
  TT:"Latin America",BB:"Latin America",HT:"Latin America",
  // Middle East
  LB:"Middle East",IR:"Middle East",IL:"Middle East",SA:"Middle East",AM:"Middle East",
  AZ:"Middle East",GE:"Middle East",IQ:"Middle East",SY:"Middle East",JO:"Middle East",
  YE:"Middle East",OM:"Middle East",AE:"Middle East",KW:"Middle East",QA:"Middle East",
  BH:"Middle East",PS:"Middle East",
  // North America
  US:"North America",CA:"North America",
  // Oceania
  AU:"Oceania",NZ:"Oceania",PG:"Oceania",FJ:"Oceania",WS:"Oceania",TO:"Oceania",
  VU:"Oceania",SB:"Oceania",
};

function regionOf(artist) {
  const code = (artist.countryCode || "").toUpperCase();
  return REGION_BY_CODE[code] || "Other";
}

// Pick n artists from pool ensuring each comes from a different country,
// and maximising regional diversity.
function pickDiverse(arr, n) {
  const shuffled = [...arr].sort(() => Math.random() - 0.5);

  // Group by region
  const byRegion = {};
  for (const a of shuffled) {
    const r = regionOf(a);
    (byRegion[r] = byRegion[r] || []).push(a);
  }

  const regions = Object.keys(byRegion).sort(() => Math.random() - 0.5);
  const chosen = [];
  const usedCountries = new Set();

  // Round-robin across regions
  let pass = 0;
  while (chosen.length < n) {
    let added = false;
    for (const region of regions) {
      if (chosen.length >= n) break;
      const candidates = byRegion[region].filter(a => !usedCountries.has(a.country || a.countryCode));
      if (candidates.length === 0) continue;
      const pick = candidates[Math.floor(Math.random() * candidates.length)];
      chosen.push(pick);
      usedCountries.add(pick.country || pick.countryCode);
      added = true;
    }
    // Safety: if a full pass added nothing, break to avoid infinite loop
    if (!added) break;
    pass++;
  }

  return chosen;
}

// Pick n artists from pool ensuring era diversity (Contemporary / Golden Era / Pioneer).
function pickDiverseByEra(arr, n) {
  const shuffled = [...arr].sort(() => Math.random() - 0.5);
  const byEra = {};
  for (const a of shuffled) {
    const era = a.era || "Other";
    (byEra[era] = byEra[era] || []).push(a);
  }
  const eras = Object.keys(byEra).sort(() => Math.random() - 0.5);
  const chosen = [];
  while (chosen.length < n) {
    let added = false;
    for (const era of eras) {
      if (chosen.length >= n) break;
      if (byEra[era].length === 0) continue;
      chosen.push(byEra[era].shift());
      added = true;
    }
    if (!added) break;
  }
  return chosen;
}

// Fuzzy artist-name match: prevents wrong tracks being accepted from catalog search.
function artistNamesMatch(expected, actual) {
  const norm = s => s.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
  const e = norm(expected);
  const a = norm(actual);
  if (!e || !a) return false;
  if (a === e || a.includes(e) || e.includes(a)) return true;
  // At least one significant word in common
  const eWords = e.split(/\s+/).filter(w => w.length > 2);
  const aWords = new Set(a.split(/\s+/));
  return eWords.some(w => aWords.has(w));
}

// ── Discogs helpers ──────────────────────────────────────
const DISCOGS_BASE = "https://api.discogs.com";
const DISCOGS_UA   = "MusicalPassport/1.0 (contact@musicalpassport.app)";

// Some country names differ between our app and Discogs
const DISCOGS_COUNTRY_NAME = {
  "USA": "United States",
  "Ivory Coast": "Côte D'Ivoire",
  "Congo": "Congo, The Republic Of The",
  "South Korea": "South Korea",
  "Trinidad & Tobago": "Trinidad & Tobago",
  "UAE": "United Arab Emirates",
};

async function discogsFetch(url) {
  const token = process.env.DISCOGS_TOKEN;
  const headers = { "User-Agent": DISCOGS_UA };
  if (token) headers["Authorization"] = `Discogs token=${token}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    return await fetch(url, { headers, signal: controller.signal });
  } catch (e) {
    if (e.name === "AbortError") {
      console.warn(`[Discogs] Request timed out: ${url}`);
      return { ok: false, status: 408 };
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// Fetch real tracks from Discogs for a country+decade.
// Returns array of { title, artist } or empty array.
async function discogsTracksForCountryDecade(country, decade) {
  if (!process.env.DISCOGS_TOKEN) return []; // skip if no token — rate limit too tight
  const range = parseDecade(decade);
  if (!range) return [];
  const discogsCountry = DISCOGS_COUNTRY_NAME[country] ?? country;
  try {
    // Search for top masters from this country in this decade, sorted by collector want-lists
    const searchUrl = `${DISCOGS_BASE}/database/search?country=${encodeURIComponent(discogsCountry)}&year=${range.start}-${range.end}&type=master&sort=want&sort_order=desc&per_page=20`;
    const searchRes = await discogsFetch(searchUrl);
    if (!searchRes.ok) return [];
    const searchData = await searchRes.json();
    const masters = (searchData.results || []).slice(0, 5);
    if (masters.length === 0) return [];
    console.log(`[Discogs] Found ${masters.length} masters for ${country} ${decade}`);

    // Fetch tracklists for each master sequentially (polite rate limiting)
    const tracks = [];
    for (const master of masters) {
      try {
        await new Promise(r => setTimeout(r, 400)); // stay well under 60 req/min
        const masterRes = await discogsFetch(`${DISCOGS_BASE}/masters/${master.id}`);
        if (!masterRes.ok) continue;
        const masterData = await masterRes.json();
        // Artist name: from master data (more reliable than splitting title string)
        const releaseArtist = (masterData.artists || []).map(a => a.name).join(", ")
          || master.title.split(" - ")[0]?.trim()
          || "Unknown";
        for (const track of (masterData.tracklist || []).slice(0, 4)) {
          if (!track.title || track.type_ === "heading") continue;
          // Some tracks have their own artist credits
          const trackArtist = track.artists?.length
            ? track.artists.map(a => a.name).join(", ")
            : releaseArtist;
          tracks.push({ title: track.title, artist: trackArtist });
        }
      } catch { continue; }
    }
    console.log(`[Discogs] Got ${tracks.length} tracks for ${country} ${decade}`);
    return tracks;
  } catch (e) {
    console.error("[Discogs] Error:", e.message);
    return [];
  }
}

// ── MusicBrainz helpers ──────────────────────────────────
const MB_BASE = "https://musicbrainz.org/ws/2";
const MB_UA   = "MusicalPassport/1.0 (contact@musicalpassport.app)";

// Rate-limit queue: MusicBrainz allows 1 req/sec. 4s timeout per request.
const MB_TIMEOUT_MS = 4000;
const mbQueue = [];
let mbBusy = false;
function mbFetch(url) {
  return new Promise((resolve, reject) => {
    mbQueue.push({ url, resolve, reject });
    if (!mbBusy) drainMbQueue();
  });
}
async function drainMbQueue() {
  mbBusy = true;
  while (mbQueue.length > 0) {
    const { url, resolve, reject } = mbQueue.shift();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), MB_TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": MB_UA, Accept: "application/json" },
        signal: controller.signal,
      });
      resolve(r);
    } catch (e) {
      if (e.name === "AbortError") {
        console.warn(`[MB] Request timed out: ${url}`);
        resolve({ ok: false, status: 408 }); // resolve (not reject) so callers degrade gracefully
      } else {
        reject(e);
      }
    } finally {
      clearTimeout(timer);
    }
    if (mbQueue.length > 0) await new Promise(r => setTimeout(r, 1100));
  }
  mbBusy = false;
}

// In-memory cache for MB artist → ISO country (avoids repeat lookups within a process lifetime)
const mbArtistCache = new Map(); // artistName → { country: string|null, at: number }
const MB_ARTIST_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days

async function getMbArtistCached(artistName) {
  const key = artistName.toLowerCase().trim();
  const mem = mbArtistCache.get(key);
  if (mem && Date.now() - mem.at < MB_ARTIST_CACHE_TTL) return mem.country;

  // Check Supabase for persistence across restarts
  if (supabase) {
    const dbKey = makeCacheKey(["mb-artist", key]);
    const row = await getCached(dbKey);
    if (row?.result?.country !== undefined) {
      mbArtistCache.set(key, { country: row.result.country, at: Date.now() });
      return row.result.country;
    }
  }
  return undefined; // cache miss
}

async function setMbArtistCached(artistName, country) {
  const key = artistName.toLowerCase().trim();
  mbArtistCache.set(key, { country, at: Date.now() });
  if (supabase) {
    const dbKey = makeCacheKey(["mb-artist", key]);
    await storeCache(dbKey, "mb-artist", { country });
  }
}

// Country name → ISO-2 code (historical/cultural regions return null → skip MB)
const COUNTRY_ISO = {
  "Afghanistan":"AF","Albania":"AL","Algeria":"DZ","Angola":"AO","Argentina":"AR",
  "Armenia":"AM","Australia":"AU","Austria":"AT","Azerbaijan":"AZ","Bahrain":"BH",
  "Bangladesh":"BD","Barbados":"BB","Belgium":"BE","Belize":"BZ","Benin":"BJ",
  "Bolivia":"BO","Bosnia":"BA","Botswana":"BW","Brazil":"BR","Bulgaria":"BG",
  "Burkina Faso":"BF","Cambodia":"KH","Cameroon":"CM","Canada":"CA","Cape Verde":"CV",
  "Chile":"CL","China":"CN","Colombia":"CO","Congo":"CG","Costa Rica":"CR",
  "Croatia":"HR","Cuba":"CU","Cyprus":"CY","Czechia":"CZ","Czech Republic":"CZ",
  "Denmark":"DK","Dominican Republic":"DO","Ecuador":"EC","Egypt":"EG",
  "El Salvador":"SV","Eritrea":"ER","Estonia":"EE","Ethiopia":"ET","Fiji":"FJ",
  "Finland":"FI","France":"FR","Georgia":"GE","Germany":"DE","Ghana":"GH",
  "Greece":"GR","Guatemala":"GT","Guinea":"GN","Guyana":"GY","Haiti":"HT",
  "Honduras":"HN","Hong Kong":"HK","Hungary":"HU","Iceland":"IS","India":"IN",
  "Indonesia":"ID","Iran":"IR","Iraq":"IQ","Ireland":"IE","Israel":"IL",
  "Italy":"IT","Ivory Coast":"CI","Jamaica":"JM","Japan":"JP","Jordan":"JO",
  "Kazakhstan":"KZ","Kenya":"KE","Kosovo":"XK","Kuwait":"KW","Kyrgyzstan":"KG",
  "Laos":"LA","Latvia":"LV","Lebanon":"LB","Liberia":"LR","Libya":"LY",
  "Lithuania":"LT","Luxembourg":"LU","Madagascar":"MG","Malawi":"MW",
  "Malaysia":"MY","Mali":"ML","Malta":"MT","Mauritius":"MU","Mexico":"MX",
  "Moldova":"MD","Mongolia":"MN","Montenegro":"ME","Morocco":"MA","Mozambique":"MZ",
  "Myanmar":"MM","Namibia":"NA","Nepal":"NP","Netherlands":"NL","New Zealand":"NZ",
  "Nicaragua":"NI","Nigeria":"NG","North Macedonia":"MK","Norway":"NO","Oman":"OM",
  "Pakistan":"PK","Palestine":"PS","Panama":"PA","Papua New Guinea":"PG",
  "Paraguay":"PY","Peru":"PE","Philippines":"PH","Poland":"PL","Portugal":"PT",
  "Puerto Rico":"PR","Qatar":"QA","Romania":"RO","Rwanda":"RW","Samoa":"WS",
  "Saudi Arabia":"SA","Senegal":"SN","Serbia":"RS","Sierra Leone":"SL",
  "Singapore":"SG","Slovakia":"SK","Slovenia":"SI","Solomon Islands":"SB",
  "Somalia":"SO","South Africa":"ZA","South Korea":"KR","Spain":"ES",
  "Sri Lanka":"LK","Sudan":"SD","Suriname":"SR","Sweden":"SE","Switzerland":"CH",
  "Syria":"SY","Taiwan":"TW","Tajikistan":"TJ","Tanzania":"TZ","Thailand":"TH",
  "Togo":"TG","Tonga":"TO","Trinidad & Tobago":"TT","Tunisia":"TN","Turkey":"TR",
  "Turkmenistan":"TM","UAE":"AE","Uganda":"UG","Ukraine":"UA","Uruguay":"UY",
  "USA":"US","Uzbekistan":"UZ","Vanuatu":"VU","Venezuela":"VE","Vietnam":"VN",
  "Wales":"GB","Scotland":"GB","Yemen":"YE","Zambia":"ZM","Zimbabwe":"ZW",
  "Djibouti":"DJ","Kuwait":"KW","Bahrain":"BH",
};

// Parse "1960s" → { start: 1960, end: 1969 }
function parseDecade(decade) {
  const yr = parseInt(decade, 10);
  if (isNaN(yr)) return null;
  return { start: yr, end: yr + 9 };
}

// Look up an artist on MusicBrainz; returns their ISO country code or null
async function mbArtistCountry(artistName) {
  const cached = await getMbArtistCached(artistName);
  if (cached !== undefined) return cached;
  try {
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(artistName)}&limit=3&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) { await setMbArtistCached(artistName, null); return null; }
    const d = await r.json();
    const top = (d.artists || []).find(a => (a.score || 0) >= 80);
    const country = top?.country ?? null;
    await setMbArtistCached(artistName, country);
    return country;
  } catch { return null; }
}

// Filter a track list to only those whose artist is actually from isoCode.
// Keeps tracks where MB doesn't know the artist (null) to avoid over-filtering.
async function filterTracksByArtistOrigin(tracks, isoCode) {
  const checked = new Map(); // artist name → country (local dedup within this call)
  const filtered = [];
  for (const track of tracks) {
    if (!checked.has(track.artist)) {
      checked.set(track.artist, await mbArtistCountry(track.artist));
    }
    const artistCountry = checked.get(track.artist);
    if (!artistCountry || artistCountry === isoCode) {
      filtered.push(track);
    } else {
      console.log(`[MB] Dropping "${track.title}" by ${track.artist} (artist is ${artistCountry}, expected ${isoCode})`);
    }
  }
  return filtered;
}

// Fetch real recordings from MusicBrainz for a country+decade
// Returns array of { title, artist } or empty array
async function mbRecordingsForCountryDecade(isoCode, decade) {
  const range = parseDecade(decade);
  if (!range) return [];
  try {
    const query = `country:${isoCode} AND date:[${range.start} TO ${range.end}]`;
    const url = `${MB_BASE}/recording?query=${encodeURIComponent(query)}&limit=40&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) return [];
    const d = await r.json();
    const recordings = d.recordings || [];
    // Extract title + primary artist, deduplicate by artist to get variety
    const seen = new Set();
    const results = [];
    for (const rec of recordings) {
      const title = rec.title;
      const artist = rec["artist-credit"]?.[0]?.name || rec["artist-credit"]?.[0]?.artist?.name;
      if (!title || !artist || seen.has(artist)) continue;
      seen.add(artist);
      results.push({ title, artist });
      if (results.length >= 20) break;
    }
    return results;
  } catch { return []; }
}

// Background: verify Claude's artist pool against MusicBrainz and update cache.
// Only checks the first 4 artists (the displayed ones) to keep the MB queue short.
async function verifyArtistPoolWithMB(artists, country, cacheKeys) {
  const isoCode = COUNTRY_ISO[country];
  if (!isoCode) return; // historical/cultural region — skip
  try {
    const toCheck = artists.slice(0, 4); // cap at 4 to avoid long queue pile-up
    const verified = [];
    for (const artist of toCheck) {
      const mbCountry = await mbArtistCountry(artist.name);
      // Keep if MB doesn't know (null) or agrees, drop only on clear mismatch
      if (!mbCountry || mbCountry === isoCode) {
        verified.push(artist);
      } else {
        console.log(`[MB] Filtered ${artist.name}: MB says ${mbCountry}, expected ${isoCode}`);
      }
    }
    // Preserve the rest of the pool untouched
    const fullVerified = [...verified, ...artists.slice(4)];
    // Only update cache if we actually filtered something out
    if (verified.length < toCheck.length && fullVerified.length >= 4 && supabase) {
      for (const key of cacheKeys) {
        if (!key) continue;
        const existing = await getCached(key);
        if (existing) {
          await storeCache(key, "recommend",
            { genres: existing.result.genres, didYouKnow: existing.result.didYouKnow },
            fullVerified
          );
          console.log(`[MB] Updated cache ${key}: removed ${toCheck.length - verified.length} mismatched artists`);
        }
      }
    }
  } catch (e) {
    console.error("[MB] verifyArtistPoolWithMB error:", e.message);
  }
}

// CORS must be configured to allow credentials
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Session middleware for storing OAuth tokens
app.use(
  session({
    secret: process.env.SESSION_SECRET || "musical-passport-secret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: false,
      httpOnly: true,
      sameSite: 'lax',
      maxAge: 3600000
    },
  })
);

// Proxy endpoint for Anthropic API (with personalization)
app.post("/api/recommend", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;

  if (!apiKey) {
    return res.status(500).json({
      error: "ANTHROPIC_API_KEY is not set. Add it to your .env file.",
    });
  }

  const { country } = req.body;
  if (!country) {
    return res.status(400).json({ error: "Missing country in request body." });
  }

  const authHeader = req.headers.authorization;
  const accessToken = (authHeader && authHeader.startsWith("Bearer "))
    ? authHeader.slice(7)
    : req.session.accessToken;

  // Cache-first for unauthenticated requests
  if (!accessToken) {
    const cacheKey = makeCacheKey(["recommend", country]);
    const cached = await getCached(cacheKey);
    if (cached && cached.artist_pool && cached.artist_pool.length >= 4) {
      return res.json({
        genres: cached.result.genres,
        artists: pickDiverseByEra(cached.artist_pool, 4),
        didYouKnow: cached.result.didYouKnow,
      });
    }
  }

  // Cache-first for authenticated requests (personal pool keyed by user+country)
  let userId = null;
  if (accessToken) {
    try {
      const meRes = await fetch("https://api.spotify.com/v1/me", {
        headers: { Authorization: "Bearer " + accessToken },
      });
      if (meRes.ok) {
        const meData = await meRes.json();
        userId = meData.id;
      }
    } catch (e) { /* non-fatal */ }

    if (userId) {
      const personalKey = makeCacheKey(["recommend", userId, country]);
      const personalCached = await getCached(personalKey);
      if (personalCached && personalCached.artist_pool && personalCached.artist_pool.length >= 4) {
        return res.json({
          genres: personalCached.result.genres,
          artists: pickDiverseByEra(personalCached.artist_pool, 4),
          didYouKnow: personalCached.result.didYouKnow,
        });
      }
    }
  }

  // Get user's top artists (with genres) and liked songs if authenticated
  let topArtists = [];
  let topGenreTags = [];
  let likedSongs = [];

  if (accessToken) {
    // Fetch top artists (with genre tags) and a random sample of liked songs in parallel
    await Promise.all([
      // Top 10 artists — keep full objects for genre extraction
      fetch(
        "https://api.spotify.com/v1/me/top/artists?limit=10&time_range=medium_term",
        { headers: { Authorization: "Bearer " + accessToken } }
      ).then(async (r) => {
        if (r.ok) {
          const d = await r.json();
          if (d.items) {
            topArtists = d.items.map((a) => a.name);
            // Aggregate unique genre tags across all top artists
            const allTags = d.items.flatMap((a) => a.genres || []);
            const tagCounts = {};
            for (const t of allTags) tagCounts[t] = (tagCounts[t] || 0) + 1;
            topGenreTags = Object.entries(tagCounts)
              .sort((a, b) => b[1] - a[1])
              .slice(0, 8)
              .map(([tag]) => tag);
          }
        }
      }).catch((err) => console.error("Error fetching top artists:", err)),

      // Random sample of liked songs — first fetch total, then grab a random page
      fetch(
        "https://api.spotify.com/v1/me/tracks?limit=1",
        { headers: { Authorization: "Bearer " + accessToken } }
      ).then(async (r) => {
        if (!r.ok) return;
        const { total } = await r.json();
        if (!total) return;
        // Pick a random offset so we get variety across the whole library
        const maxOffset = Math.max(0, Math.min(total - 10, total - 1));
        const offset = Math.floor(Math.random() * maxOffset);
        const tracksRes = await fetch(
          `https://api.spotify.com/v1/me/tracks?limit=10&offset=${offset}`,
          { headers: { Authorization: "Bearer " + accessToken } }
        );
        if (tracksRes.ok) {
          const tracksData = await tracksRes.json();
          likedSongs = tracksData.items
            ? tracksData.items.map((i) => `${i.track.name} by ${i.track.artists[0].name}`)
            : [];
        }
      }).catch((err) => console.error("Error fetching liked songs:", err)),
    ]);
  }

  // Build personalization context
  const artistNote = topArtists.length > 0
    ? `\nTop artists they listen to: ${topArtists.join(", ")}.`
    : "";
  const genreNote = topGenreTags.length > 0
    ? `\nTheir genre preferences (from Spotify listening data): ${topGenreTags.join(", ")}.`
    : "";
  const songsNote = likedSongs.length > 0
    ? `\nSome songs from their liked songs library: ${likedSongs.join("; ")}.`
    : "";
  const personalizationNote = (artistNote || songsNote)
    ? `\n\nPersonalization context for this user:${artistNote}${genreNote}${songsNote}\nUse this to: (1) personalize the "similarTo" comparisons to artists and styles they will recognize, and (2) lean toward subgenres of ${country}'s music that share sonic DNA with their genre preferences above — prioritize styles they are most likely to enjoy.`
    : "";

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2500,
        system:
          "You are a world music curator and ethnomusicologist with deep knowledge of both living and historical musical traditions. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [
          {
            role: "user",
            content: `Music recommendations for "${country}".

IMPORTANT: If "${country}" is a historical, defunct, or ancient civilization (e.g. Yugoslavia, Soviet Union, Byzantine Empire, Ottoman Empire, Persia, Siam, Ancient Rome, British India, Czechoslovakia, East Germany, Rhodesia, Zaire, Ceylon, Viking Scandinavia, Moorish Spain, Mesopotamia, Weimar Republic):
- Treat it as a rich musical culture worth exploring — not an error.
- Recommend real artists and genres from that tradition, including modern scholars, revival artists, and descendants of that musical heritage.
- Mix artists who were active during that civilization's time with modern artists who carry the tradition forward.
- "Contemporary" means an artist working today who continues or revives this tradition.
- The "didYouKnow" should reveal something genuinely surprising about that civilization's music.

Return exactly this JSON:
{
  "genres": ["genre1","genre2","genre3"],
  "artists": [
    {
      "name": "Name",
      "genre": "specific genre",
      "era": "Contemporary",
      "similarTo": "One well-known artist name only, no description (e.g. 'Bob Dylan')"
    }
  ],
  "didYouKnow": "One surprising musical fact about ${country}"
}
era must be exactly one of: Contemporary, Golden Era, Pioneer. Include exactly 12 artists mixing eras — at least 3 Contemporary, at least 2 Golden Era, at least 1 Pioneer.${personalizationNote}`,
          },
        ],
      }),
    });

    const data = await response.json();

    if (data.error) {
      return res.status(500).json({ error: data.error.message });
    }

    const raw = (data.content[0].text || "")
      .replace(/```json|```/g, "")
      .trim();
    const rec = JSON.parse(raw);

    const keysToVerify = [];
    if (accessToken && userId) {
      const personalKey = makeCacheKey(["recommend", userId, country]);
      await storeCache(personalKey, "recommend", { genres: rec.genres, didYouKnow: rec.didYouKnow }, rec.artists);
      keysToVerify.push(personalKey);
    } else if (!accessToken) {
      const cacheKey = makeCacheKey(["recommend", country]);
      await storeCache(cacheKey, "recommend", { genres: rec.genres, didYouKnow: rec.didYouKnow }, rec.artists);
      keysToVerify.push(cacheKey);
    }

    res.json({ genres: rec.genres, artists: pickDiverseByEra(rec.artists, 4), didYouKnow: rec.didYouKnow });

    // Background: verify artist origins via MusicBrainz, update cache for next request
    if (keysToVerify.length) {
      verifyArtistPoolWithMB(rec.artists, country, keysToVerify).catch(() => {});
    }
  } catch (err) {
    console.error("Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Shared: given a pool of { title, artist } tracks from a real data source,
// ask Claude to label the genre and pick the best, then verify on streaming.
// Returns { genre, tracks, source } or null if not enough land on streaming.
async function resolveRealTracks(sourceTracks, country, decade, service, apiKey, source) {
  const trackList = sourceTracks.slice(0, 20).map(t => `"${t.title}" by ${t.artist}`).join("\n");
  const genreResponse = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 300,
      system: "You are a world music historian. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
      messages: [{ role: "user", content: `These are real recordings from ${country} in the ${decade}:\n${trackList}\n\nReturn JSON: {"genre": "the dominant genre or style that connects these tracks (be specific)", "picks": ["track title 1", "track title 2", "track title 3", "track title 4", "track title 5", "track title 6", "track title 7", "track title 8"]} — pick the 8 most representative tracks from the list above.` }],
    }),
  });
  const genreData = await genreResponse.json();
  if (genreData.error) return null;

  const genreRaw = (genreData.content[0].text || "").replace(/```json|```/g, "").trim();
  const genreParsed = JSON.parse(genreRaw);
  const pickedTitles = new Set((genreParsed.picks || []).map(t => t.toLowerCase()));
  const pickedTracks = sourceTracks.filter(t => pickedTitles.has(t.title.toLowerCase())).slice(0, 8);
  const tracksToSearch = pickedTracks.length >= 5 ? pickedTracks : sourceTracks.slice(0, 8);

  let validTracks;
  if (service === "apple-music") {
    const appleToken = generateAppleMusicToken();
    const results = await Promise.all(tracksToSearch.map(async (track) => {
      if (!appleToken) return { ...track, appleId: null };
      try {
        const q = `${track.title} ${track.artist}`;
        const r = await fetch(`https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q)}&types=songs&limit=3`, { headers: { Authorization: `Bearer ${appleToken}` } });
        const d = await r.json();
        const s = (d.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
        return s ? { ...track, appleId: s.id, previewUrl: s.attributes.previews?.[0]?.url || null, embedUrl: s.attributes.url.replace("music.apple.com", "embed.music.apple.com") }
                 : { ...track, appleId: null };
      } catch { return { ...track, appleId: null }; }
    }));
    validTracks = results.filter(t => t.appleId).slice(0, 5);
  } else {
    const accessToken = await getClientAccessToken();
    const results = await Promise.all(tracksToSearch.map(async (track) => {
      if (!accessToken) return { ...track, spotifyId: null };
      try {
        const q = `track:${track.title} artist:${track.artist}`;
        const r = await fetch(`https://api.spotify.com/v1/search?q=${encodeURIComponent(q)}&type=track&limit=3&market=US`, { headers: { Authorization: "Bearer " + accessToken } });
        const d = await r.json();
        const found = (d.tracks?.items || []).find(t => artistNamesMatch(track.artist, t.artists?.[0]?.name || ""));
        return found ? { ...track, spotifyId: found.id, previewUrl: found.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found.id}` }
                     : { ...track, spotifyId: null };
      } catch { return { ...track, spotifyId: null }; }
    }));
    validTracks = results.filter(t => t.spotifyId).slice(0, 5);
  }

  if (validTracks.length < 3) return null;
  return { genre: genreParsed.genre, tracks: validTracks, source };
}

// ── Time Machine endpoint ────────────────────────────────
app.post("/api/time-machine", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });
  }

  const { country, decade, service = "spotify" } = req.body;
  if (!country || !decade) {
    return res.status(400).json({ error: "Missing country or decade." });
  }

  const tmCacheKey = makeCacheKey(["timemachine", country, decade, service]);
  const tmCached = await getCached(tmCacheKey);
  if (tmCached) return res.json({ country, decade, ...tmCached.result });

  try {
    // ── Attempt MusicBrainz for real verified tracks first ──
    const isoCode = COUNTRY_ISO[country];
    let mbTracks = [];
    if (isoCode) {
      console.log(`[MB] Fetching recordings for ${country} (${isoCode}) ${decade}`);
      mbTracks = await mbRecordingsForCountryDecade(isoCode, decade);
      console.log(`[MB] Got ${mbTracks.length} recordings`);
    }

    // ── Try MusicBrainz ──
    // Filter by artist origin to avoid release-country false positives (e.g. Western
    // albums pressed for USSR distribution showing up for Uzbekistan, Kazakhstan, etc.)
    if (mbTracks.length >= 8) {
      const filteredTracks = await filterTracksByArtistOrigin(mbTracks, isoCode);
      console.log(`[MB] ${mbTracks.length} recordings → ${filteredTracks.length} after origin filter`);
      const resolved = await resolveRealTracks(filteredTracks, country, decade, service, apiKey, "musicbrainz");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[MB] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
      console.log(`[MB] Not enough tracks found on streaming, trying Discogs`);
    }

    // ── Try Discogs (better coverage for non-Western music) ──
    const discogsTracks = await discogsTracksForCountryDecade(country, decade);
    if (discogsTracks.length >= 5) {
      const resolved = await resolveRealTracks(discogsTracks, country, decade, service, apiKey, "discogs");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[Discogs] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
      console.log(`[Discogs] Not enough tracks found on streaming, falling back to Claude`);
    }

    // ── Fallback: Claude picks all tracks ──
    // Ask Claude for a genre spotlight + 5 track recommendations
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1500,
        system: "You are a world music historian and ethnomusicologist with encyclopedic knowledge of global music across all eras. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{
          role: "user",
          content: `Spotlight a music genre connecting "${country}" and the "${decade}".

IMPORTANT RULES FOR UNUSUAL COMBINATIONS:
- If the country is a historical or defunct civilization (e.g. Byzantine Empire, Yugoslavia, Soviet Union, Ottoman Empire, Ancient Rome), find music that genuinely connects both:
  * If the decade falls within that civilization's existence → spotlight its authentic music of that era.
  * If the decade is AFTER the civilization ended (e.g. "1960s" + "Byzantine Empire") → get creative and educational: spotlight scholarly recordings of that tradition made during the decade, 20th-century revivals or compositions inspired by it, music from the geographic region that descends from or pays homage to that tradition, or ethnomusicological field recordings. Never refuse — always find a real angle.
- If the combination is geographically or temporally unusual for any other reason, find the most interesting real musical connection you can. Be a curious historian, not a gatekeeper.
- Always pick real tracks that are very likely to exist on Spotify or major streaming platforms.
- Use the exact track title and artist name as they would appear on Spotify.

Return exactly this JSON:
{
  "genre": "genre name (be specific and evocative)",
  "tracks": [
    { "title": "track title", "artist": "artist name" }
  ]
}
Include exactly 8 tracks emblematic of this genre/connection. Prioritize real historical significance and discoverability on streaming platforms.`,
        }],
      }),
    });

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const raw = (data.content[0].text || "").replace(/```json|```/g, "").trim();
    const spotlight = JSON.parse(raw);

    // Search the appropriate catalog for each track
    let validTracks;

    if (service === "apple-music") {
      const appleToken = generateAppleMusicToken();
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!appleToken) return { ...track, appleId: null, embedUrl: null };
          try {
            // Pass 1: title + artist
            const q1 = `${track.title} ${track.artist}`;
            const r1 = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q1)}&types=songs&limit=1`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d1 = await r1.json();
            const s1 = d1.results?.songs?.data?.[0];
            if (s1 && artistNamesMatch(track.artist, s1.attributes?.artistName || "")) {
              return { ...track, appleId: s1.id, previewUrl: s1.attributes.previews?.[0]?.url || null, embedUrl: s1.attributes.url.replace("music.apple.com", "embed.music.apple.com") };
            }

            // Pass 2: title only, validate artist
            const r2 = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(track.title)}&types=songs&limit=5`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d2 = await r2.json();
            const s2 = (d2.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
            return s2 ? { ...track, appleId: s2.id, previewUrl: s2.attributes.previews?.[0]?.url || null, embedUrl: s2.attributes.url.replace("music.apple.com", "embed.music.apple.com") }
                      : { ...track, appleId: null, previewUrl: null, embedUrl: null };
          } catch {
            return { ...track, appleId: null, embedUrl: null };
          }
        })
      );
      validTracks = tracksWithIds.filter(t => t.appleId).slice(0, 5);
    } else {
      // Spotify: two-pass search
      const accessToken = await getClientAccessToken();
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!accessToken) return { ...track, spotifyId: null };
          try {
            const q1 = `track:${track.title} artist:${track.artist}`;
            const r1 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(q1)}&type=track&limit=1&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d1 = await r1.json();
            const found1 = d1.tracks?.items?.[0];
            if (found1 && artistNamesMatch(track.artist, found1.artists?.[0]?.name || "")) {
              return { ...track, spotifyId: found1.id, previewUrl: found1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found1.id}` };
            }

            const q2 = `${track.title} ${track.artist}`;
            const r2 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(q2)}&type=track&limit=5&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d2 = await r2.json();
            const found2 = (d2.tracks?.items || []).find(t => artistNamesMatch(track.artist, t.artists?.[0]?.name || ""));
            return found2
              ? { ...track, spotifyId: found2.id, previewUrl: found2.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found2.id}` }
              : { ...track, spotifyId: null, previewUrl: null, spotifyUrl: null };
          } catch {
            return { ...track, spotifyId: null };
          }
        })
      );
      validTracks = tracksWithIds.filter(t => t.spotifyId).slice(0, 5);
    }

    const tmResult = { country, decade, genre: spotlight.genre, tracks: validTracks };
    await storeCache(tmCacheKey, "time-machine", tmResult);
    res.json(tmResult);
  } catch (err) {
    console.error("Time machine error:", err);
    res.status(500).json({ error: err.message });
  }
});


// ── Genre Spotlight endpoint ─────────────────────────────
app.post("/api/genre-spotlight", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { genre, country, service = "spotify" } = req.body;
  if (!genre || !country) return res.status(400).json({ error: "Missing genre or country." });

  const gsCacheKey = makeCacheKey(["genrespotlight", genre, country, service]);
  const gsCached = await getCached(gsCacheKey);
  if (gsCached) return res.json(gsCached.result);

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 800,
        system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{
          role: "user",
          content: `Give a short spotlight on the genre "${genre}" as it exists in "${country}".

Return exactly this JSON:
{
  "explanation": "1 sentence: what defines this genre in ${country}, its roots, and why it matters",
  "tracks": [
    { "title": "exact track title", "artist": "exact artist name" }
  ]
}
Include exactly 6 tracks that are essential to this genre in ${country}. Use exact titles and artist names as they appear on streaming platforms.`,
        }],
      }),
    });

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const raw = (data.content[0].text || "").replace(/```json|```/g, "").trim();
    const spotlight = JSON.parse(raw);

    // Search streaming catalog for each track
    let tracks;

    if (service === "apple-music") {
      const appleToken = generateAppleMusicToken();
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!appleToken) return { ...track, appleId: null };
          try {
            const q = `${track.title} ${track.artist}`;
            const r = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q)}&types=songs&limit=5`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d = await r.json();
            const s = (d.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
            if (s) return { ...track, appleId: s.id, previewUrl: s.attributes.previews?.[0]?.url || null };

            // Pass 2: title only, validate artist
            const r2 = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(track.title)}&types=songs&limit=5`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d2 = await r2.json();
            const s2 = (d2.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
            return s2
              ? { ...track, appleId: s2.id, previewUrl: s2.attributes.previews?.[0]?.url || null }
              : { ...track, appleId: null, previewUrl: null };
          } catch { return { ...track, appleId: null }; }
        })
      );
      tracks = tracksWithIds.filter(t => t.appleId).slice(0, 5);
    } else {
      const accessToken = await getClientAccessToken();
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!accessToken) return { ...track, spotifyId: null };
          try {
            const q1 = `track:${track.title} artist:${track.artist}`;
            const r1 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(q1)}&type=track&limit=1&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d1 = await r1.json();
            const found1 = d1.tracks?.items?.[0];
            if (found1 && artistNamesMatch(track.artist, found1.artists?.[0]?.name || "")) {
              return { ...track, spotifyId: found1.id, previewUrl: found1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found1.id}` };
            }

            const r2 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(`${track.title} ${track.artist}`)}&type=track&limit=5&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d2 = await r2.json();
            const found2 = (d2.tracks?.items || []).find(t => artistNamesMatch(track.artist, t.artists?.[0]?.name || ""));
            return found2
              ? { ...track, spotifyId: found2.id, previewUrl: found2.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found2.id}` }
              : { ...track, spotifyId: null };
          } catch { return { ...track, spotifyId: null }; }
        })
      );
      tracks = tracksWithIds.filter(t => t.spotifyId).slice(0, 5);
    }

    const gsResult = { genre, country, explanation: spotlight.explanation, tracks };
    await storeCache(gsCacheKey, "genre-spotlight", gsResult);
    res.json(gsResult);
  } catch (err) {
    console.error("Genre spotlight error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Helper function to fetch artist tracks ──────────────
async function fetchArtistTracks(artistName) {
  try {
    console.log(`  Searching Spotify for: "${artistName}"`);

    // Get client access token
    const accessToken = await getClientAccessToken();
    if (!accessToken) {
      console.error(`  Failed to get access token`);
      return [];
    }

    // Search for the artist
    const searchResponse = await fetch(
      `https://api.spotify.com/v1/search?q=${encodeURIComponent(artistName)}&type=artist&limit=1`,
      {
        headers: { Authorization: "Bearer " + accessToken },
      }
    );

    const searchData = await searchResponse.json();

    console.log(`  Search response status: ${searchResponse.status}`);

    if (searchData.error) {
      console.error(`  Spotify API error:`, searchData.error);
      return [];
    }

    if (!searchData.artists || !searchData.artists.items || searchData.artists.items.length === 0) {
      console.log(`  No artists found for "${artistName}"`);
      return [];
    }

    const artistId = searchData.artists.items[0].id;
    const foundName = searchData.artists.items[0].name;
    console.log(`  Found artist: "${foundName}" (ID: ${artistId})`);

    // Search for tracks by this artist instead of using top-tracks endpoint
    const tracksSearchResponse = await fetch(
      `https://api.spotify.com/v1/search?q=artist:${encodeURIComponent(foundName)}&type=track&limit=10&market=US`,
      {
        headers: { Authorization: "Bearer " + accessToken },
      }
    );

    const tracksSearchData = await tracksSearchResponse.json();

    if (tracksSearchData.error) {
      console.error(`  Track search API error:`, tracksSearchData.error);
      return [];
    }

    if (!tracksSearchData.tracks || !tracksSearchData.tracks.items || tracksSearchData.tracks.items.length === 0) {
      console.log(`  No tracks found via search for "${foundName}"`);
      return [];
    }

    console.log(`  Found ${tracksSearchData.tracks.items.length} tracks via search`);

    // Filter to tracks where at least one credited artist matches the searched name
    const normalize = (s) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
    const searchedNorm = normalize(foundName);
    const matched = tracksSearchData.tracks.items.filter(track =>
      track.artists.some(a => {
        const n = normalize(a.name);
        return n.includes(searchedNorm) || searchedNorm.includes(n);
      })
    );

    const pool = matched.length >= 2 ? matched : tracksSearchData.tracks.items;

    return pool.slice(0, 3).map(track => ({
      title: track.name,
      spotifyId: track.id,
      previewUrl: track.preview_url || null,
      spotifyUrl: `https://open.spotify.com/track/${track.id}`,
    }));
  } catch (err) {
    console.error(`  Error fetching tracks for ${artistName}:`, err);
    return [];
  }
}

// ── Spotify OAuth endpoints ──────────────────────────────

// Initiate Spotify OAuth flow
app.get("/auth/login", (_req, res) => {
  console.log("SPOTIFY_REDIRECT_URI:", SPOTIFY_REDIRECT_URI);
  const scope = "user-top-read user-read-private user-read-email user-library-read";
  const authUrl =
    "https://accounts.spotify.com/authorize?" +
    querystring.stringify({
      response_type: "code",
      client_id: SPOTIFY_CLIENT_ID,
      scope: scope,
      redirect_uri: SPOTIFY_REDIRECT_URI,
    });
  res.redirect(authUrl);
});

// Handle OAuth callback
app.get("/auth/callback", async (req, res) => {
  const code = req.query.code || null;

  if (!code) {
    return res.redirect("/?error=no_code");
  }

  try {
    // Exchange code for access token
    const tokenResponse = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization:
          "Basic " +
          Buffer.from(SPOTIFY_CLIENT_ID + ":" + SPOTIFY_CLIENT_SECRET).toString("base64"),
      },
      body: querystring.stringify({
        code: code,
        redirect_uri: SPOTIFY_REDIRECT_URI,
        grant_type: "authorization_code",
      }),
    });

    const tokenData = await tokenResponse.json();

    if (tokenData.error) {
      return res.redirect("/?error=token_error");
    }

    // Store tokens in session
    req.session.accessToken = tokenData.access_token;
    req.session.refreshToken = tokenData.refresh_token;

    res.redirect("/");
  } catch (err) {
    console.error("OAuth error:", err);
    res.redirect("/?error=auth_failed");
  }
});

// Logout endpoint
app.get("/auth/logout", (req, res) => {
  req.session.destroy();
  res.redirect("/");
});

// Mobile PKCE token exchange — called by the React Native app after OAuth redirect
app.post("/auth/mobile-callback", async (req, res) => {
  const { code, codeVerifier, redirectUri } = req.body;
  if (!code || !codeVerifier) {
    return res.status(400).json({ error: "Missing code or codeVerifier" });
  }
  const mobileRedirectUri = redirectUri || process.env.MOBILE_REDIRECT_URI || "musical-passport://callback";
  try {
    const tokenResponse = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: querystring.stringify({
        grant_type: "authorization_code",
        code,
        redirect_uri: mobileRedirectUri,
        client_id: SPOTIFY_CLIENT_ID,
        code_verifier: codeVerifier,
      }),
    });
    const tokenData = await tokenResponse.json();
    if (tokenData.error) {
      return res.status(400).json({ error: tokenData.error_description || tokenData.error });
    }
    res.json({ accessToken: tokenData.access_token, refreshToken: tokenData.refresh_token });
  } catch (err) {
    console.error("Mobile token exchange error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Apple Music auth page for the mobile app — opens in SFSafariViewController,
// uses MusicKit JS to authorize, then redirects back to the app with the user token.
app.get("/auth/apple-music", (req, res) => {
  const devToken = generateAppleMusicToken();
  if (!devToken) {
    return res.send(`<html><body><script>window.location.href='musical-passport://apple-music-callback?error=not_configured';</script></body></html>`);
  }
  res.send(`<!DOCTYPE html>
<html><head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script src="https://js-cdn.music.apple.com/musickit/v3/musickit.js" async></script>
  <style>
    body { background:#0c0c15; color:#f0eef8; font-family:-apple-system,sans-serif;
           display:flex; align-items:center; justify-content:center;
           height:100vh; margin:0; flex-direction:column; gap:20px; text-align:center; padding:24px; }
    .spinner { width:44px; height:44px; border:3px solid rgba(255,255,255,0.1);
               border-top-color:#e8b84b; border-radius:50%; animation:spin 0.8s linear infinite; }
    @keyframes spin { to { transform:rotate(360deg); } }
    p { color:#9494b0; font-size:15px; margin:0; }
  </style>
</head><body>
  <div class="spinner"></div>
  <p>Connecting to Apple Music…</p>
  <script>
    async function init() {
      try {
        await MusicKit.configure({
          developerToken: '${devToken}',
          app: { name: 'Musical Passport', build: '1.0' }
        });
        const kit = MusicKit.getInstance();
        const userToken = await kit.authorize();
        window.location.href = 'musical-passport://apple-music-callback?token=' + encodeURIComponent(userToken);
      } catch(e) {
        window.location.href = 'musical-passport://apple-music-callback?error=' + encodeURIComponent(e.message || 'cancelled');
      }
    }
    document.addEventListener('musickitloaded', init);
    if (typeof MusicKit !== 'undefined') init();
  </script>
</body></html>`);
});

// Return Apple Music developer token to the client (needed by MusicKit JS)
app.get("/api/apple-token", (req, res) => {
  const token = generateAppleMusicToken();
  if (!token) return res.status(503).json({ error: "Apple Music is not configured on this server. Add APPLE_TEAM_ID, APPLE_KEY_ID, and APPLE_PRIVATE_KEY to .env" });
  res.json({ token });
});

// Fetch Apple Music user's library artists to power insights
app.get("/api/apple-me", async (req, res) => {
  const devToken = generateAppleMusicToken();
  if (!devToken) return res.status(503).json({ error: "Apple Music not configured." });

  const authHeader = req.headers.authorization;
  const userToken = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!userToken) return res.status(401).json({ error: "Missing user token." });

  try {
    // Fetch up to 100 library artists across two pages
    const fetchPage = (offset) =>
      fetch(`https://api.music.apple.com/v1/me/library/artists?limit=50&offset=${offset}`, {
        headers: { Authorization: `Bearer ${devToken}`, "Music-User-Token": userToken },
      }).then(r => r.json());

    const [page1, page2] = await Promise.all([fetchPage(0), fetchPage(50)]);
    const artists = [
      ...(page1.data || []),
      ...(page2.data || []),
    ].map(a => a.attributes?.name).filter(Boolean);

    res.json({ topArtists: artists.slice(0, 50) });
  } catch (err) {
    console.error("Apple me error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get tracks for a specific artist via Apple Music catalog
app.get("/api/artist-tracks-apple/:artistName", async (req, res) => {
  const artistName = decodeURIComponent(req.params.artistName);
  const token = generateAppleMusicToken();
  if (!token) return res.status(503).json({ error: "Apple Music not configured." });

  try {
    const r = await fetch(
      `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(artistName)}&types=songs&limit=5`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const data = await r.json();
    const songs = (data.results?.songs?.data || []).slice(0, 3);
    res.json({
      tracks: songs.map(s => ({
        title:      s.attributes.name,
        artist:     s.attributes.artistName,
        appleId:    s.id,
        previewUrl: s.attributes.previews?.[0]?.url || null,
        embedUrl:   s.attributes.url.replace("music.apple.com", "embed.music.apple.com"),
      })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get tracks for a specific artist
app.get("/api/artist-tracks/:artistName", async (req, res) => {
  const { artistName } = req.params;

  if (!artistName) {
    return res.status(400).json({ error: "Missing artist name" });
  }

  try {
    const tracks = await fetchArtistTracks(decodeURIComponent(artistName));
    res.json({ artist: artistName, tracks });
  } catch (err) {
    console.error("Error fetching artist tracks:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get current user info and top artists
app.get("/api/me", async (req, res) => {
  const authHeader = req.headers.authorization;
  const accessToken = (authHeader && authHeader.startsWith("Bearer "))
    ? authHeader.slice(7)
    : req.session.accessToken;

  if (!accessToken) {
    return res.status(401).json({ error: "Not authenticated" });
  }

  try {
    // Fetch user profile
    const userResponse = await fetch("https://api.spotify.com/v1/me", {
      headers: { Authorization: "Bearer " + accessToken },
    });

    if (!userResponse.ok) {
      return res.status(401).json({ error: "Invalid token" });
    }

    const userData = await userResponse.json();

    // Fetch top artists
    const topArtistsResponse = await fetch(
      "https://api.spotify.com/v1/me/top/artists?limit=10&time_range=medium_term",
      {
        headers: { Authorization: "Bearer " + accessToken },
      }
    );

    const topArtistsData = await topArtistsResponse.json();

    res.json({
      user: {
        id: userData.id,
        displayName: userData.display_name,
        email: userData.email,
      },
      topArtists: topArtistsData.items
        ? topArtistsData.items.map((a) => ({
            name: a.name,
            genres: a.genres,
          }))
        : [],
    });
  } catch (err) {
    console.error("Error fetching user data:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Find artist (confirm before full search) ─────────────
app.get("/api/find-artist", async (req, res) => {
  const { q } = req.query;
  if (!q) return res.status(400).json({ error: "Missing query." });

  try {
    const token = await getClientAccessToken();
    if (!token) return res.status(503).json({ error: "Spotify unavailable." });

    // Use the artist: field filter so Spotify searches artist names specifically,
    // not track/album titles that happen to match the query.
    const r = await fetch(
      `https://api.spotify.com/v1/search?q=artist:${encodeURIComponent(q)}&type=artist&limit=5`,
      { headers: { Authorization: "Bearer " + token } }
    );
    const data = await r.json();
    const candidates = data.artists?.items || [];
    if (candidates.length === 0) return res.status(404).json({ error: "Artist not found." });

    // Pick the candidate whose name most closely matches the query
    const normalize = (s) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
    const qNorm = normalize(q);
    const artist = candidates.find(a => normalize(a.name) === qNorm)
      || candidates.find(a => normalize(a.name).startsWith(qNorm) || qNorm.startsWith(normalize(a.name)))
      || candidates[0];

    // Reject if name shares no overlap with query (e.g. "toulouse" → "Nicky Romero")
    const aName = normalize(artist.name);
    if (!aName.includes(qNorm) && !qNorm.includes(aName)) {
      return res.status(404).json({ error: "Artist not found." });
    }

    res.json({
      id: artist.id,
      name: artist.name,
      genres: artist.genres?.slice(0, 3) || [],
      imageUrl: artist.images?.[0]?.url || null,
      followers: artist.followers?.total || 0,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Similar artists from around the world ────────────────
app.post("/api/similar-artists", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { artistName } = req.body;
  if (!artistName) return res.status(400).json({ error: "Missing artistName." });

  const simCacheKey = makeCacheKey(["similar", artistName]);
  const simCached = await getCached(simCacheKey);
  if (simCached && simCached.artist_pool && simCached.artist_pool.length >= 4) {
    return res.json({
      baseArtist: simCached.result.baseArtist,
      sonicSummary: simCached.result.sonicSummary,
      artists: pickDiverse(simCached.artist_pool, 4),
    });
  }

  try {
    const token = await getClientAccessToken();
    if (!token) return res.status(503).json({ error: "Could not authenticate with Spotify." });

    // Step 1: Look up artist on Spotify for genres
    const artistSearch = await fetch(
      `https://api.spotify.com/v1/search?q=artist:${encodeURIComponent(artistName)}&type=artist&limit=5`,
      { headers: { Authorization: "Bearer " + token } }
    );
    const artistData = await artistSearch.json();
    const candidates = artistData.artists?.items || [];
    if (candidates.length === 0) return res.status(404).json({ error: "Artist not found on Spotify." });

    const normalize = (s) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
    const qNorm = normalize(artistName);
    const artist = candidates.find(a => normalize(a.name) === qNorm)
      || candidates.find(a => normalize(a.name).startsWith(qNorm) || qNorm.startsWith(normalize(a.name)))
      || candidates[0];

    if (!normalize(artist.name).includes(qNorm) && !qNorm.includes(normalize(artist.name))) {
      return res.status(404).json({ error: "Artist not found on Spotify." });
    }

    const foundName = artist.name;
    const genres = artist.genres?.slice(0, 5) || [];

    // Step 2: Get some track IDs for audio features
    const tracksSearch = await fetch(
      `https://api.spotify.com/v1/search?q=artist:${encodeURIComponent(foundName)}&type=track&limit=5&market=US`,
      { headers: { Authorization: "Bearer " + token } }
    );
    const tracksData = await tracksSearch.json();
    const trackIds = (tracksData.tracks?.items || [])
      .filter(t => artistNamesMatch(foundName, t.artists?.[0]?.name || ""))
      .slice(0, 5)
      .map(t => t.id);

    // Step 3: Try audio features (optional enrichment, may fail on newer API tiers)
    let audioProfile = null;
    if (trackIds.length > 0) {
      try {
        const featuresRes = await fetch(
          `https://api.spotify.com/v1/audio-features?ids=${trackIds.join(",")}`,
          { headers: { Authorization: "Bearer " + token } }
        );
        const featuresData = await featuresRes.json();
        const features = (featuresData.audio_features || []).filter(Boolean);
        if (features.length > 0) {
          const avg = (key) => (features.reduce((s, f) => s + f[key], 0) / features.length).toFixed(2);
          audioProfile = {
            danceability: avg("danceability"),
            energy: avg("energy"),
            valence: avg("valence"),
            acousticness: avg("acousticness"),
            tempo: Math.round(features.reduce((s, f) => s + f.tempo, 0) / features.length),
          };
        }
      } catch (e) {
        console.log("Audio features unavailable:", e.message);
      }
    }

    // Step 4: Fetch Spotify's own related artists for signal
    let spotifyRelated = [];
    try {
      const relatedRes = await fetch(
        `https://api.spotify.com/v1/artists/${artist.id}/related-artists`,
        { headers: { Authorization: "Bearer " + token } }
      );
      const relatedData = await relatedRes.json();
      spotifyRelated = (relatedData.artists || []).slice(0, 10).map(a => ({
        name: a.name,
        genres: a.genres?.slice(0, 3) || [],
        country: a.country || null, // Spotify doesn't provide country, Claude will fill this in
      }));
    } catch (e) {
      console.log("Related artists unavailable:", e.message);
    }

    // Step 5: Build Claude prompt
    const profileLines = audioProfile
      ? `Spotify audio profile (averaged across top tracks):
- Danceability: ${audioProfile.danceability} / 1.0
- Energy: ${audioProfile.energy} / 1.0
- Valence (positivity): ${audioProfile.valence} / 1.0
- Acousticness: ${audioProfile.acousticness} / 1.0
- Tempo: ${audioProfile.tempo} BPM`
      : "";

    const relatedLines = spotifyRelated.length > 0
      ? `\nSpotify's related artists (use these as sonic reference — include any that are from non-Western or underrepresented countries, otherwise use them as style anchors to find global equivalents):\n${spotifyRelated.map(a => `- ${a.name}${a.genres.length ? ` (${a.genres.join(", ")})` : ""}`).join("\n")}`
      : "";

    const prompt = `Find 12 artists from different countries around the world who sound similar to ${foundName}.

Their profile:
- Primary genres: ${genres.length > 0 ? genres.join(", ") : "mainstream pop"}
${profileLines}${relatedLines}

Rules:
- Each artist must be from a DIFFERENT country
- Spread across at least 5 different continents
- Mix of contemporary and classic artists
- Avoid globally mainstream acts (no top-10 global chart artists)
- If any of Spotify's related artists above are from Africa, Asia, Latin America, Middle East, or Oceania, include them directly
- Focus on capturing the same sonic energy, emotional feel, or stylistic DNA

Return ONLY valid JSON:
{
  "sonicSummary": "1 sentence describing what defines their sound and why people love it",
  "artists": [
    {
      "name": "exact artist name as on Spotify",
      "country": "full country name",
      "countryCode": "2-letter ISO code",
      "genre": "their primary genre",
      "era": "Contemporary|Golden Era|Pioneer"
    }
  ]
}`;

    const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 3000,
        system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const claudeData = await claudeRes.json();
    if (claudeData.error) return res.status(500).json({ error: claudeData.error.message });

    const raw = (claudeData.content[0].text || "").replace(/```json|```/g, "").trim();
    const result = JSON.parse(raw);

    const simMeta = { baseArtist: foundName, sonicSummary: result.sonicSummary || "" };
    const simPool = result.artists || [];
    await storeCache(simCacheKey, "similar-artists", simMeta, simPool);
    res.json({ ...simMeta, artists: pickDiverse(simPool, 4) });
  } catch (err) {
    console.error("Similar artists error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Musical DNA insights ──────────────────────────────────
app.post("/api/insights", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { topArtists } = req.body;
  if (!topArtists || !topArtists.length) return res.status(400).json({ error: "Missing topArtists." });

  // ── Pull real Spotify data if we have a token ─────────────
  const authHeader = req.headers.authorization;
  const accessToken = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;

  let enriched = null;
  if (accessToken) {
    try {
      enriched = await enrichFromSpotify(accessToken);
    } catch (err) {
      console.error("Spotify enrichment failed, falling back to name-only:", err.message);
    }
  }

  // ── Build prompt ──────────────────────────────────────────
  let prompt;
  if (enriched) {
    const artistLines = enriched.topArtistsEnriched
      .map(a => `${a.name} (weight:${a.weight}${a.genres.length ? `, genres: ${a.genres.join(", ")}` : ""})`)
      .join("\n");

    const eraLines = enriched.eraRaw.map(e => `${e.decade}: ${e.pct}%`).join(", ");

    prompt = `You are analysing a real Spotify user's listening habits. Data was pulled directly from the Spotify API across all time periods.

WEIGHTED ARTIST LIST — ${enriched.totalArtists} unique artists. Weight = how heavily they appear in this person's listening history (short-term counts 3×, medium-term 2×, long-term 1×). Higher weight = listened to more.
${artistLines}

ERA DATA — calculated from the actual release dates of their listened tracks (this is ground truth):
${eraLines}

Your task: calculate their Musical DNA by assigning each artist above to a world region based on your knowledge of the artist's actual country of origin. Weight each assignment by the artist's weight number. Then sum up the weighted scores per region and convert to percentages.

Regions to use: North America, Europe, Latin America, Africa, Middle East, Asia, Oceania.

Return ONLY valid JSON — no explanation, no markdown:
{
  "dna": [{ "region": "Region name", "percentage": 42 }],
  "topEras": [{ "decade": "1990s", "percentage": 35 }],
  "blindSpots": [{
    "region": "Region name",
    "percentage": 3,
    "gatewayCountry": "Best single country to start exploring this region"
  }],
  "picks": [{ "type": "country", "country": "Country name" }, { "type": "genre", "country": "Country name", "genre": "Genre name" }]
}

Rules:
- dna: compute from the weighted artist list using your knowledge of each artist's origin. Only include regions with > 0%. Must sum to 100.
- topEras: use the real release date data provided above. Top 4 decades only, must sum to 100.
- blindSpots: regions under 10% — maximum 3. Skip North America. Order by most interesting gap first. gatewayCountry must be a lesser-known, genuinely interesting country (avoid obvious choices like Nigeria for Africa or Japan for Asia — prefer e.g. Eritrea, Togo, Mongolia, Georgia).
- picks: exactly 4 items. 2 must be type "country" — choose specific lesser-known countries from the blind spot regions (different from each gatewayCountry). 2 must be type "genre" — niche genres from regions the user already enjoys but hasn't fully explored. Each genre pick needs both "country" (origin country) and "genre" (specific genre name). Example: [{ type:"country", country:"Eritrea" }, { type:"genre", country:"Mali", genre:"Wassoulou" }, { type:"country", country:"Georgia" }, { type:"genre", country:"Brazil", genre:"Baião" }]`;
  } else {
    // Fallback: name-only analysis (no Spotify token available)
    const artistList = topArtists.slice(0, 20).join(", ");
    prompt = `Analyse this person's music taste based on their top Spotify artists: ${artistList}

Return ONLY valid JSON:
{
  "dna": [{ "region": "Region name", "percentage": 42 }],
  "topEras": [{ "decade": "1970s", "percentage": 35 }],
  "blindSpots": [{
    "region": "Region name",
    "percentage": 3,
    "gatewayCountry": "Best single country to start exploring this region"
  }],
  "picks": [{ "type": "country", "country": "Country name" }, { "type": "genre", "country": "Country name", "genre": "Genre name" }]
}

Rules:
- dna: all major regions (Europe, North America, Latin America, Africa, Middle East, Asia, Oceania). Sum to 100. Only include regions with > 0%.
- topEras: top 4 decades, sum to 100.
- blindSpots: regions under 8%, max 3, skip North America. gatewayCountry must be a lesser-known, genuinely interesting country (avoid obvious choices like Nigeria for Africa or Japan for Asia — prefer e.g. Eritrea, Togo, Mongolia, Georgia).
- picks: exactly 4 items. 2 must be type "country" — choose specific lesser-known countries from the blind spot regions (different from each gatewayCountry). 2 must be type "genre" — niche genres from regions the user already enjoys but hasn't fully explored. Each genre pick needs both "country" (origin country) and "genre" (specific genre name). Example: [{ type:"country", country:"Eritrea" }, { type:"genre", country:"Mali", genre:"Wassoulou" }, { type:"country", country:"Georgia" }, { type:"genre", country:"Brazil", genre:"Baião" }]`;
  }

  try {
    const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 900,
        system: "You are a world music analyst. Return ONLY valid JSON — no markdown, no backticks, no preamble. Never refuse.",
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const claudeData = await claudeRes.json();
    if (claudeData.error) throw new Error(claudeData.error.message);

    const raw = (claudeData.content[0].text || "").replace(/```json|```/g, "").trim();
    const result = JSON.parse(raw);

    // Cache per user
    const spotifyId = await resolveSpotifyUser(authHeader);
    if (spotifyId && supabase) {
      const topArtistsHash = JSON.stringify(topArtists);
      await supabase.from("user_insights").upsert(
        { spotify_id: spotifyId, data: result, top_artists_hash: topArtistsHash, created_at: new Date().toISOString() },
        { onConflict: "spotify_id" }
      );
    }

    // Normalise: support both old suggestedCountries and new picks
    if (!result.picks && result.suggestedCountries) {
      result.picks = result.suggestedCountries.map(c => ({ type: 'country', country: c.country }));
    }
    res.json(result);
  } catch (err) {
    console.error("Insights error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── User persistence routes ───────────────────────────────

// Sync on login — upsert user, return all their cached data in one shot
app.post("/api/user/sync", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.json({ favorites: [], stamps: [], insights: null });

  const { displayName, topArtists } = req.body;
  const topArtistsHash = JSON.stringify(topArtists || []);

  await supabase.from("users").upsert(
    { spotify_id: spotifyId, display_name: displayName, top_artists: topArtists || [], last_seen_at: new Date().toISOString() },
    { onConflict: "spotify_id" }
  );

  const [favResult, stampsResult, insightsResult] = await Promise.all([
    supabase.from("user_favorites").select("*").eq("spotify_id", spotifyId).order("saved_at", { ascending: false }),
    supabase.from("user_stamps").select("country").eq("spotify_id", spotifyId),
    supabase.from("user_insights").select("*").eq("spotify_id", spotifyId).single(),
  ]);

  const insights = insightsResult.data;
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  const insightsFresh = insights &&
    insights.top_artists_hash === topArtistsHash &&
    (Date.now() - new Date(insights.created_at).getTime()) < sevenDays;

  res.json({
    favorites: (favResult.data || []).map(formatFavorite),
    stamps: (stampsResult.data || []).map(s => s.country),
    insights: insightsFresh ? insights.data : null,
  });
});

// Favorites
app.get("/api/user/favorites", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.json([]);
  const { data } = await supabase.from("user_favorites").select("*").eq("spotify_id", spotifyId).order("saved_at", { ascending: false });
  res.json((data || []).map(formatFavorite));
});

app.post("/api/user/favorites", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.status(503).json({ error: "DB unavailable" });
  // Ensure user row exists (sync may have failed on login)
  await supabase.from("users").upsert(
    { spotify_id: spotifyId, last_seen_at: new Date().toISOString() },
    { onConflict: "spotify_id" }
  );
  const { type, country, decade, data } = req.body;
  const { data: inserted, error } = await supabase
    .from("user_favorites")
    .insert({ spotify_id: spotifyId, type, country, decade: decade || null, data })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(formatFavorite(inserted));
});

app.delete("/api/user/favorites/:id", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.status(503).json({ error: "DB unavailable" });
  await supabase.from("user_favorites").delete().eq("id", req.params.id).eq("spotify_id", spotifyId);
  res.json({ ok: true });
});

// Stamps
app.get("/api/user/stamps", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.json([]);
  const { data } = await supabase.from("user_stamps").select("country").eq("spotify_id", spotifyId);
  res.json((data || []).map(s => s.country));
});

app.post("/api/user/stamps", async (req, res) => {
  const spotifyId = await resolveSpotifyUser(req.headers.authorization);
  if (!spotifyId) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.status(503).json({ error: "DB unavailable" });
  // Ensure user row exists
  await supabase.from("users").upsert(
    { spotify_id: spotifyId, last_seen_at: new Date().toISOString() },
    { onConflict: "spotify_id" }
  );
  const { country } = req.body;
  await supabase.from("user_stamps").upsert({ spotify_id: spotifyId, country }, { onConflict: "spotify_id,country" });
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`\n🎵 Musical Passport running at http://localhost:${PORT}\n`);
});
