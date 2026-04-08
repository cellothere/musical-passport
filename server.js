require("dotenv").config();
const express = require("express");
const cors = require("cors");
const fetch = require("node-fetch");
const path = require("path");
const session = require("express-session");
const querystring = require("querystring");
const { createSign, createHash } = require("crypto");
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
let tokenFetchInFlight = null; // deduplicates concurrent refresh requests

// Get client credentials access token for public API access
async function getClientAccessToken() {
  // Return cached token if still valid
  if (clientAccessToken && tokenExpiry && Date.now() < tokenExpiry) {
    return clientAccessToken;
  }

  // If a fetch is already in flight, wait for it instead of firing another
  if (tokenFetchInFlight) return tokenFetchInFlight;

  tokenFetchInFlight = (async () => {
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
    } finally {
      tokenFetchInFlight = null;
    }
  })();

  return tokenFetchInFlight;
}

// ── Artist image URL fetching (Spotify → Apple Music → Last.fm fallback) ──
// Priority: Apple Music → Last.fm → Spotify (last resort, dev-mode quota is precious).
// Spotify is only tried if not currently rate-limited and skipSpotify is false.
async function fetchArtistImageUrl(artistName, { skipSpotify = false, genre = null } = {}) {
  const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const target = normalise(artistName);

  // When the artist name is short or common, ambiguous matches (e.g. "Lego" → toy brand) are likely.
  // Appending the genre to the search term helps disambiguate. We try genre-qualified first, then fall back.
  const isAmbiguous = artistName.trim().split(/\s+/).length <= 2;
  const terms = (genre && isAmbiguous)
    ? [`${artistName} ${genre}`, artistName]
    : [artistName];

  // 1. Apple Music — no quota concerns, high-quality artwork
  for (const term of terms) {
    try {
      const appleToken = generateAppleMusicToken();
      if (appleToken) {
        const r = await fetch(
          `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(term)}&types=artists&limit=5`,
          { headers: { Authorization: `Bearer ${appleToken}` } }
        );
        if (r.ok) {
          const data = await r.json();
          const candidates = data.results?.artists?.data || [];
          const match = candidates.find(a => normalise(a.attributes?.name || '') === target)
            ?? candidates.find(a => {
              const n = normalise(a.attributes?.name || '');
              return n.includes(target) || target.includes(n);
            });
          const artworkUrl = match?.attributes?.artwork?.url;
          if (artworkUrl) return artworkUrl.replace('{w}', '600').replace('{h}', '600');
        }
      }
    } catch {}
  }

  // 2. Last.fm — global coverage, good for non-Western artists
  try {
    const lfKey = process.env.LASTFM_API_KEY;
    if (lfKey) {
      const r = await fetch(
        `https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(artistName)}&autocorrect=1&api_key=${lfKey}&format=json`
      );
      const data = await r.json();
      const images = data.artist?.image || [];
      const large = images.find(i => i.size === 'extralarge' || i.size === 'mega');
      const url = (large || images[images.length - 1])?.['#text'];
      // Last.fm default placeholder hash — skip it
      if (url && !url.includes('2a96cbd8b46e442fc41c2b86b821562f')) return url;
    }
  } catch {}

  // 3. Deezer — high-quality artist images, no auth, no quota cost vs Spotify
  for (const term of terms) {
    try {
      const deezerImageUrl = await deezerEnqueue(async () => {
        const r = await fetch(`${DEEZER_BASE}/search/artist?q=${encodeURIComponent(term)}&limit=5`);
        if (!r.ok) return null;
        const d = await r.json();
        if (d.error?.code === 4) return null;
        const artists = d.data || [];
        const match = artists.find(a => normalise(a.name) === target)
          ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || target.includes(n); });
        return match?.picture_xl || match?.picture_big || null;
      });
      if (deezerImageUrl) return deezerImageUrl;
    } catch {}
  }

  // 4. Spotify — last resort only; dev-mode quota is very limited (30 req/30s window)
  if (!skipSpotify && Date.now() >= spotifyRateLimitedUntil) try {
    const spotifyImageUrl = await spotifyEnqueue(async () => {
      const token = await getClientAccessToken();
      if (!token) return null;
      const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
      const qNorm = normalize(artistName);
      const r = await spotifyFetch(
        `https://api.spotify.com/v1/search?q=${encodeURIComponent(artistName)}&type=artist&limit=5`,
        { headers: { Authorization: 'Bearer ' + token } }
      );
      if (!r.ok) return null;
      const data = await r.json();
      const candidates = data.artists?.items || [];
      if (candidates.length === 0) return null;
      const match = candidates.find(a => normalize(a.name) === qNorm)
        || candidates.find(a => normalize(a.name).startsWith(qNorm) || qNorm.startsWith(normalize(a.name)))
        || candidates[0];
      if (!match || !(normalize(match.name).includes(qNorm) || qNorm.includes(normalize(match.name)))) return null;
      if (genre && match.genres && match.genres.length > 0) {
        const spotifyGenreStr = match.genres.join(' ').toLowerCase();
        const genreWords = genre.toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(w => w.length > 3);
        const hasOverlap = genreWords.length === 0 || genreWords.some(w => spotifyGenreStr.includes(w));
        if (!hasOverlap) return null;
      }
      return match.images?.[0]?.url || null;
    });
    if (spotifyImageUrl) return spotifyImageUrl;
  } catch {}

  return null;
}

// ── Artist tracks in-memory cache ────────────────────────
const artistTracksMemCache = new Map(); // key → { tracks, cachedAt }
const ARTIST_TRACKS_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// ── Spotify API queue (rate-limit protection) ────────────
// Spotify client-credentials endpoints allow ~30 req/s but burst 429s under
// concurrent load. Queue all client-credentials calls sequentially with a
// small gap between them so we never fire two at once.
const spotifyQueue = [];
let spotifyBusy = false;
// Circuit breaker: when a non-retryable 429 is received, all queued calls are
// short-circuited until this timestamp. Prevents draining 10+ queued calls that
// will all hit 429 anyway and worsen the ban.
let spotifyRateLimitedUntil = 0;

function spotifyEnqueue(fn) {
  return new Promise((resolve, reject) => {
    spotifyQueue.push({ fn, resolve, reject });
    if (!spotifyBusy) drainSpotifyQueue();
  });
}

const RATE_LIMIT_ERROR = new Response(JSON.stringify({ error: "rate_limited" }), {
  status: 429,
  headers: { "Content-Type": "application/json" },
});

async function drainSpotifyQueue() {
  if (spotifyQueue.length === 0) { spotifyBusy = false; return; }
  spotifyBusy = true;
  const { fn, resolve, reject } = spotifyQueue.shift();

  // Circuit breaker: if we're inside a rate-limit window, short-circuit immediately
  if (Date.now() < spotifyRateLimitedUntil) {
    const remainingS = Math.ceil((spotifyRateLimitedUntil - Date.now()) / 1000);
    console.warn(`  [Spotify] circuit open — skipping queued call (${remainingS}s remaining, ${spotifyQueue.length} more queued)`);
    resolve(RATE_LIMIT_ERROR.clone());
    setTimeout(drainSpotifyQueue, 50); // drain remaining items quickly
    return;
  }

  try { resolve(await fn()); } catch (e) { reject(e); }
  // 500ms between calls → max ~2/s = ~60 per 30s window.
  // Spotify dev mode limits are ~30 req per 30s rolling window; staying at 2/s gives headroom.
  // Extended quota mode can handle much higher throughput if/when approved.
  setTimeout(drainSpotifyQueue, 500);
}

// Wraps fetch with retry-on-429 logic.
// If Retry-After > MAX_RETRY_WAIT_S we don't retry — Spotify issued a long-term
// ban (up to 24h on some endpoints). Opens the circuit breaker so queued calls
// short-circuit instead of all hitting 429.
const MAX_RETRY_WAIT_S = 30;
async function spotifyFetch(url, options = {}, retries = 2) {
  const res = await fetch(url, options);
  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("Retry-After") || "1", 10);
    if (retries > 0 && retryAfter <= MAX_RETRY_WAIT_S) {
      const wait = (retryAfter + 1) * 1000;
      console.log(`  [Spotify] 429 – waiting ${wait}ms before retry`);
      await new Promise(r => setTimeout(r, wait));
      return spotifyFetch(url, options, retries - 1);
    }
    // Long-term ban — open circuit breaker so remaining queued calls don't pile on
    spotifyRateLimitedUntil = Date.now() + retryAfter * 1000;
    console.warn(`  [Spotify] 429 rate-limited (Retry-After ${retryAfter}s) – circuit open until ${new Date(spotifyRateLimitedUntil).toISOString()}`);
    return new Response(JSON.stringify({ error: "rate_limited", retryAfter }), {
      status: 429,
      headers: { "Content-Type": "application/json" },
    });
  }
  return res;
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
  const joined = parts.filter(Boolean).join("_").toLowerCase();
  const hasNonLatin = /[^\u0000-\u007f]/.test(joined);
  const normalized = joined
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // ó→o, ú→u, ø→o, etc.
    .replace(/\s+/g, "-").replace(/[^a-z0-9_-]/g, "");
  if (hasNonLatin) {
    // Append an 8-char hash so non-Latin scripts get a unique, stable key
    const hash = createHash("md5").update(joined).digest("hex").slice(0, 8);
    return (normalized || "x") + "_" + hash;
  }
  return normalized;
}

// ─── Genre canonicalization ───────────────────────────────────────────────────
// Resolves user-facing genre strings to a consistent canonical form so that
// "polo disco" and "disco polo", "drum 'n' bass" and "drum & bass", "kawwali"
// and "qawwali" all resolve to the same cache key and the same Last.fm tag.

// Known aliases: covers phonetic variants and common abbreviations that
// word-sort or punctuation normalisation alone can't catch.
const GENRE_ALIASES = new Map([
  // Qawwali spelling variants
  ["kawwali",          "qawwali"],
  ["kwaali",           "qawwali"],
  ["kawali",           "qawwali"],
  ["qawali",           "qawwali"],
  // Hip-hop
  ["hip hop",          "hip-hop"],
  ["hiphop",           "hip-hop"],
  // R&B
  ["r and b",          "r&b"],
  ["rhythm and blues", "r&b"],
  ["rnb",              "r&b"],
  // Drum and bass
  ["dnb",              "drum and bass"],
  // Reggaetón
  ["reggaeton",        "reggaetón"],
  // Séga (Mauritius/Réunion)
  ["sega",             "séga"],
  // Zydeco
  ["zeideco",          "zydeco"],
  ["zaideco",          "zydeco"],
  // Cumbia
  ["cumbia",           "cumbia"],    // keep to normalise accented variants
]);

/** Strip punctuation/formatting noise so "drum 'n' bass" → "drum and bass" */
function preNormalizeGenre(genre) {
  return genre
    .toLowerCase()
    .trim()
    .replace(/[''ʼ]/g, "")               // remove apostrophes: drum 'n' bass → drum n bass
    .replace(/\bn\b/g, "and")            // isolated "n" → "and": drum n bass → drum and bass
    .replace(/\s*&\s*/g, " and ")        // & → and: drum & bass → drum and bass
    .replace(/\s+/g, " ")
    .trim();
}

// In-memory cache: pre-normalised string → canonical display name
const genreNormCache = new Map();

/**
 * Returns the canonical display name for a genre string.
 * Falls back to the original if Last.fm doesn't recognise any candidate.
 */
async function resolveGenreCanonical(genre) {
  if (!genre) return genre;

  const norm = preNormalizeGenre(genre);

  if (genreNormCache.has(norm)) return genreNormCache.get(norm);

  // 1. Known aliases table (phonetic / abbreviation variants)
  if (GENRE_ALIASES.has(norm)) {
    const canonical = GENRE_ALIASES.get(norm);
    genreNormCache.set(norm, canonical);
    return canonical;
  }

  // 2. Build candidate list to try against Last.fm
  //    Word-sort catches ordering variants: "polo disco" → ["disco","polo"] → "disco polo"
  const wordSorted = norm.split(" ").sort().join(" ");
  const candidates = [...new Set([norm, wordSorted])]; // deduplicate

  const lfKey = process.env.LASTFM_API_KEY;
  if (lfKey) {
    for (const candidate of candidates) {
      try {
        const url = `https://ws.audioscrobbler.com/2.0/?method=tag.getInfo&tag=${encodeURIComponent(candidate)}&api_key=${lfKey}&format=json`;
        const r = await fetch(url);
        const data = await r.json();
        if (!data.error && data.tag?.name) {
          // Last.fm returns its own canonical casing (e.g. "Disco Polo", "drum and bass")
          const canonical = data.tag.name;
          genreNormCache.set(norm, canonical);
          if (norm !== preNormalizeGenre(genre)) genreNormCache.set(preNormalizeGenre(genre), canonical);
          return canonical;
        }
      } catch { /* network glitch — fall through */ }
    }
  }

  // 3. Fall back to the original string (preserves user-visible casing)
  genreNormCache.set(norm, genre);
  return genre;
}

async function getCached(cacheKey) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from("recommendation_cache")
    .select("*")
    .eq("cache_key", cacheKey)
    .single();
  if (error || !data) return null;
  // Treat expired rows as cache misses
  if (data.expires_at && new Date(data.expires_at) < new Date()) return null;
  supabase.from("recommendation_cache")
    .update({ hit_count: data.hit_count + 1, last_accessed_at: new Date().toISOString() })
    .eq("cache_key", cacheKey)
    .then(() => {});
  return data;
}

const CACHE_TTL = {
  'recommend':          7   * 24 * 60 * 60 * 1000,
  'genre-spotlight':    180 * 24 * 60 * 60 * 1000,
  'genre-deeper':       180 * 24 * 60 * 60 * 1000,
  'time-machine':       14  * 24 * 60 * 60 * 1000,
  'artist-tracks':      90  * 24 * 60 * 60 * 1000,
  'artist-tracks-apple':90  * 24 * 60 * 60 * 1000,
  'similar-artists':    180 * 24 * 60 * 60 * 1000,
  'similar-of':         180 * 24 * 60 * 60 * 1000,
  'genre-artists':      180 * 24 * 60 * 60 * 1000,
};

async function storeCache(cacheKey, endpoint, result, artistPool = null) {
  if (!supabase) return;
  const ttlMs = CACHE_TTL[endpoint];
  const expires_at = ttlMs ? new Date(Date.now() + ttlMs).toISOString() : null;
  await supabase.from("recommendation_cache").upsert(
    { cache_key: cacheKey, endpoint, result, artist_pool: artistPool, expires_at },
    { onConflict: "cache_key" }
  );
}

// Remove artists from a pool whose artist-tracks cache is flagged empty.
async function filterOutFlaggedArtists(artistPool) {
  if (!supabase || !artistPool?.length) return artistPool || [];
  const slugs = artistPool.map(a => makeCacheKey(["artist-tracks-apple", a.name]));
  const { data } = await supabase
    .from("recommendation_cache")
    .select("cache_key, result")
    .in("cache_key", slugs);
  const flaggedKeys = new Set(
    (data || []).filter(r => r.result?.flagged && r.result?.tracks?.length === 0).map(r => r.cache_key)
  );
  return artistPool.filter(a => !flaggedKeys.has(makeCacheKey(["artist-tracks-apple", a.name])));
}

// For each artist in a fresh Claude recommendation, verify they have playable tracks.
// Only checks up to VERIFY_LIMIT uncached artists to keep Spotify API usage low.
// Uses Apple Music for the verify pass; Spotify search fallback runs lazily on card flip.
// Returns only artists that have at least 1 track.
const VERIFY_LIMIT = 5;
async function verifyArtistTracksForRecommend(artists, country) {
  const appleToken = generateAppleMusicToken();

  const results = await Promise.all(artists.map(async (artist) => {
    const cacheKey = makeCacheKey(["artist-tracks-apple", artist.name]);

    // Always pass through artists already cached with real tracks — no API call needed
    const existing = artistTracksMemCache.get(cacheKey);
    if (existing && existing.tracks.length > 0) return { artist, tracks: existing.tracks, cached: true };
    const dbCached = await getCached(cacheKey);
    if (dbCached?.result?.tracks?.length > 0) return { artist, tracks: dbCached.result.tracks, cached: true };

    return { artist, tracks: null, cached: false };
  }));

  // Split: already verified vs needs a live check
  const verified = results.filter(r => r.cached);
  const needsCheck = results.filter(r => !r.cached);

  // Only verify up to VERIFY_LIMIT uncached artists — rest pass through (Spotify handles on card flip)
  const toCheck = needsCheck.slice(0, VERIFY_LIMIT);
  const passThrough = needsCheck.slice(VERIFY_LIMIT);

  console.log(`  [recommend-verify] ${verified.length} cached, checking ${toCheck.length}/${needsCheck.length} uncached (${passThrough.length} deferred)`);

  const checked = await Promise.all(toCheck.map(async ({ artist }) => {
    const cacheKey = makeCacheKey(["artist-tracks-apple", artist.name]);

    // Apple Music only for the verify pass — Spotify fallback happens lazily on card flip
    const appleTracks = await proactiveArtistTracks(artist.name, [], appleToken);
    const uniqueTracks = appleTracks.filter((t, i, a) =>
      a.findIndex(x => x.title.toLowerCase() === t.title.toLowerCase()) === i
    );

    if (uniqueTracks.length > 0) {
      const withDeezer = await enrichWithDeezer(uniqueTracks, artist.name);
      artistTracksMemCache.set(cacheKey, { tracks: withDeezer, cachedAt: Date.now() });
      storeCache(cacheKey, "artist-tracks-apple", { tracks: withDeezer }).catch(() => {});
      return { artist, tracks: withDeezer };
    }

    // Flag it — cron will deep-enrich; Spotify check runs when user taps the card
    storeCache(cacheKey, "artist-tracks-apple", { tracks: [], flagged: true }).catch(() => {});
    console.log(`  [recommend-verify] no tracks for "${artist.name}" (${country}) — flagged`);
    return { artist, tracks: [] };
  }));

  const allResults = [
    ...verified,
    ...checked,
    ...passThrough.map(r => ({ ...r, tracks: [] })), // deferred artists pass through as unverified
  ];

  // Keep artists with tracks OR deferred ones (they get a chance on card flip)
  return allResults
    .filter(r => r.tracks === null || r.tracks.length > 0 || passThrough.some(p => p.artist.name === r.artist.name))
    .map(r => r.artist);
}

// ── Flag review ───────────────────────────────────────────

/**
 * Delete all cache entries matching a country (but not artist-track caches,
 * which are expensive to regenerate and are country-agnostic).
 */
async function bustCountryCache(country) {
  if (!supabase) return;
  const pattern = `%${makeCacheKey([country])}%`;
  const { error } = await supabase
    .from("recommendation_cache")
    .delete()
    .like("cache_key", pattern)
    .not("cache_key", "like", "artist-tracks%");
  if (!error) console.log(`[flag-review] busted cache entries matching "${country}"`);
}

/**
 * Ask Claude for replacement tracks from a country/genre, then resolve them
 * against Spotify or Apple Music. Uses spotifyEnqueue for Spotify calls.
 */
async function getReplacementTracks(country, genre, existingTracks, count, apiKey, service) {
  if (count <= 0 || !apiKey) return [];
  const existingArtists = [...new Set(existingTracks.map(t => t.artist).filter(Boolean))];

  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 400,
      system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks.",
      messages: [{
        role: "user",
        content: `I need up to ${count} replacement tracks for the "${genre}" genre from ${country}.

CRITICAL: Every artist MUST be born in or genuinely from ${country}. Return fewer tracks — or an empty list — rather than include any foreign artist.
${existingArtists.length ? `Do not repeat: ${existingArtists.join(", ")}.` : ""}

Return exactly this JSON:
{
  "tracks": [
    { "title": "exact track title", "artist": "exact artist name from ${country}" }
  ]
}`,
      }],
    }),
  });

  const data = await response.json();
  if (data.error) { console.error("[flag-review] Claude replacement error:", data.error.message); return []; }

  let suggestions;
  try {
    suggestions = JSON.parse((data.content[0].text || "").replace(/```json|```/g, "").trim());
  } catch { return []; }
  if (!suggestions.tracks?.length) return [];

  const found = [];

  if (service === "apple-music") {
    const appleToken = generateAppleMusicToken();
    if (!appleToken) return [];
    for (const track of suggestions.tracks) {
      try {
        await new Promise(r => setTimeout(r, 350)); // polite Apple Music delay
        const q = `${track.title} ${track.artist}`;
        const r = await fetch(
          `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q)}&types=songs&limit=5`,
          { headers: { Authorization: `Bearer ${appleToken}` } }
        );
        const d = await r.json();
        const s = (d.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
        if (s) found.push({ ...track, appleId: s.id, previewUrl: s.attributes.previews?.[0]?.url || null });
      } catch { /* skip */ }
    }
  } else {
    // Spotify — all calls go through the rate-limit queue
    for (const track of suggestions.tracks) {
      try {
        const result = await spotifyEnqueue(async () => {
          const accessToken = await getClientAccessToken();
          if (!accessToken) return null;

          // Pass 1: strict field search
          const r1 = await spotifyFetch(
            `https://api.spotify.com/v1/search?q=${encodeURIComponent(`track:${track.title} artist:${track.artist}`)}&type=track&limit=1&market=US`,
            { headers: { Authorization: "Bearer " + accessToken } }
          );
          const d1 = await r1.json();
          const hit1 = d1.tracks?.items?.[0];
          if (hit1 && artistNamesMatch(track.artist, hit1.artists?.[0]?.name || "")) {
            return { ...track, spotifyId: hit1.id, previewUrl: hit1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${hit1.id}` };
          }

          // Pass 2: looser search
          const r2 = await spotifyFetch(
            `https://api.spotify.com/v1/search?q=${encodeURIComponent(`${track.title} ${track.artist}`)}&type=track&limit=5&market=US`,
            { headers: { Authorization: "Bearer " + accessToken } }
          );
          const d2 = await r2.json();
          const hit2 = (d2.tracks?.items || []).find(t => artistNamesMatch(track.artist, t.artists?.[0]?.name || ""));
          if (hit2) {
            return { ...track, spotifyId: hit2.id, previewUrl: hit2.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${hit2.id}` };
          }
          return null;
        });
        if (result) found.push(result);
      } catch { /* skip */ }
    }
    return found.filter(t => t?.spotifyId);
  }

  return found;
}

/**
 * Verify and repair a single (country, genre) cache entry.
 * Uses Claude to audit all tracks, removes confirmed-wrong ones,
 * and fetches replacements from the streaming catalog.
 *
 * Returns { fixed: boolean, verified: number, rejected: number }
 */
async function reviewGenreSpotlightContext({ country, genre, flags }, apiKey) {
  const services = ["spotify", "apple-music"];
  let anyFixed = false;
  let totalVerified = 0;
  let totalRejected = 0;

  for (const service of services) {
    const cacheKey = makeCacheKey(["genrespotlight", genre, country, service]);
    const cached = await getCached(cacheKey);

    const flagComments = flags.filter(f => f.comment).map(f => `"${f.comment}"`).join(", ");
    const flaggedSummary = [...new Set(flags.map(f => `"${f.track_title}" by ${f.track_artist || "Unknown"}`))].join(", ");

    // No cached tracks — verify just the flagged ones to decide whether to bust
    if (!cached?.result?.tracks?.length) {
      const noCache = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
        body: JSON.stringify({
          model: "claude-sonnet-4-20250514",
          max_tokens: 300,
          system: "You are a world music fact-checker. Return ONLY valid JSON — no markdown, no backticks.",
          messages: [{
            role: "user",
            content: `Expert testers flagged these tracks as wrong for the genre "${genre}" from ${country}:\n${flaggedSummary}\nTester comments: ${flagComments || "none"}\n\nAre any of these tracks genuinely problematic (wrong country of origin or wrong genre)? Return: {"any_problematic": true, "reason": "one-line explanation"} or {"any_problematic": false, "reason": "..."}`,
          }],
        }),
      });
      const noCacheData = await noCache.json();
      if (!noCacheData.error) {
        let verdict;
        try { verdict = JSON.parse((noCacheData.content[0].text || "").replace(/```json|```/g, "").trim()); } catch { /* skip */ }
        if (verdict?.any_problematic) {
          await supabase.from("recommendation_cache").delete().eq("cache_key", cacheKey);
          console.log(`[flag-review] ✓ Cache busted (was empty) for ${genre}/${country} (${service}): ${verdict.reason}`);
          anyFixed = true;
          totalRejected += flags.length;
        } else {
          console.log(`[flag-review] No issues found for ${genre}/${country} (${service}) — no cache, flags appear legitimate`);
        }
      }
      continue;
    }

    const currentTracks = cached.result.tracks;

    // Ask Claude to audit every track in the cached result
    const verifyResponse = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 700,
        system: "You are a world music fact-checker. Return ONLY valid JSON — no markdown, no backticks.",
        messages: [{
          role: "user",
          content: `Audit these "${genre}" tracks currently cached for ${country}. Expert listeners flagged problems.

Cached tracks:
${currentTracks.map((t, i) => `${i + 1}. "${t.title}" by ${t.artist ?? "Unknown"}`).join("\n")}

Flagged by testers: ${flaggedSummary}
Tester comments: ${flagComments || "none"}

For EVERY track above, determine:
- Is the artist genuinely born in or from ${country}?
- Does this track represent "${genre}" as it exists specifically in ${country}?

A track must meet BOTH criteria to be verified. If uncertain, reject it.

Return exactly this JSON:
{
  "verified": [{ "title": "...", "artist": "..." }],
  "rejected": [{ "title": "...", "artist": "...", "reason": "one-line reason" }],
  "explanation_update": "updated 1-2 sentence explanation for ${country}'s relationship to ${genre}, or null to keep existing"
}`,
        }],
      }),
    });

    const verifyData = await verifyResponse.json();
    if (verifyData.error) {
      console.error(`[flag-review] Claude audit error for ${genre}/${country}:`, verifyData.error.message);
      continue;
    }

    let audit;
    try {
      audit = JSON.parse((verifyData.content[0].text || "").replace(/```json|```/g, "").trim());
    } catch {
      console.error(`[flag-review] Failed to parse audit for ${genre}/${country} (${service})`);
      continue;
    }

    totalVerified += audit.verified?.length ?? 0;
    totalRejected += audit.rejected?.length ?? 0;

    for (const r of audit.rejected || []) {
      console.log(`[flag-review]   ✗ ${service} — "${r.title}" by ${r.artist}: ${r.reason}`);
    }

    if (!audit.rejected?.length) {
      console.log(`[flag-review] No issues found for ${genre}/${country} (${service}) — flagged tracks may have already been evicted`);
      continue;
    }

    // Keep the tracks Claude verified; discard the rest
    const rejectedTitlesLower = new Set((audit.rejected || []).map(r => r.title.toLowerCase().trim()));
    const keptTracks = currentTracks.filter(t => !rejectedTitlesLower.has((t.title || "").toLowerCase().trim()));

    // Request replacements to bring the list back to up to 6 tracks
    let newTracks = keptTracks;
    const replacementsNeeded = Math.max(0, 6 - keptTracks.length);
    if (replacementsNeeded > 0) {
      console.log(`[flag-review] Requesting ${replacementsNeeded} replacement(s) for ${genre}/${country} (${service})`);
      // Small pause between Claude calls to avoid rapid-fire requests
      await new Promise(r => setTimeout(r, 1500));
      const replacements = await getReplacementTracks(country, genre, keptTracks, replacementsNeeded, apiKey, service);
      newTracks = [...keptTracks, ...replacements].slice(0, 6);
      console.log(`[flag-review] Got ${replacements.length} replacement track(s)`);
    }

    const updatedResult = {
      ...cached.result,
      tracks: newTracks,
      ...(audit.explanation_update ? { explanation: audit.explanation_update } : {}),
    };
    await storeCache(cacheKey, "genre-spotlight", updatedResult);
    console.log(`[flag-review] ✓ Cache updated: ${genre}/${country} (${service}) — kept ${keptTracks.length}, added ${newTracks.length - keptTracks.length}`);
    anyFixed = true;
  }

  return { fixed: anyFixed, verified: totalVerified, rejected: totalRejected };
}

/**
 * Main flag-review loop. Called by the cron job.
 * Groups pending flags by (country, genre) context, reviews each context,
 * marks flags as reviewed, and logs outcomes.
 *
 * @param {number} maxContexts  Max distinct (country, genre) pairs to process per run.
 *                              Keep low (2–3) to stay within cron timeouts and API limits.
 */
async function processFlaggedTracks(maxContexts = 2) {
  if (!supabase) return { reviewed: 0, fixed: 0 };
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) { console.warn("[flag-review] No ANTHROPIC_API_KEY — skipping"); return { reviewed: 0, fixed: 0 }; }

  // Fetch all unreviewed flags, ordered oldest first so nothing starves
  const { data: flags, error } = await supabase
    .from("track_flags")
    .select("*")
    .is("reviewed_at", null)
    .order("created_at", { ascending: true })
    .limit(200);

  if (error) { console.error("[flag-review] DB error:", error.message); return { reviewed: 0, fixed: 0 }; }
  if (!flags?.length) { console.log("[flag-review] No pending flags"); return { reviewed: 0, fixed: 0 }; }

  console.log(`[flag-review] ${flags.length} unreviewed flag(s)`);

  // Group by context: genre-spotlight flags by (country, genre), others by country alone
  const contexts = new Map();
  for (const flag of flags) {
    const key = flag.genre
      ? `genre:${flag.country}|||${flag.genre}`
      : `country:${flag.country}`;
    if (!contexts.has(key)) {
      contexts.set(key, {
        type: flag.genre ? "genre" : "country",
        country: flag.country,
        genre: flag.genre || null,
        flags: [],
      });
    }
    contexts.get(key).flags.push(flag);
  }

  let reviewed = 0;
  let fixed = 0;
  let processed = 0;

  for (const [, ctx] of contexts) {
    if (processed >= maxContexts) break;

    const label = ctx.genre ? `${ctx.genre} / ${ctx.country}` : ctx.country;
    console.log(`[flag-review] Processing: ${label} (${ctx.flags.length} flag(s))`);

    let reviewResult = "reviewed";
    try {
      if (ctx.type === "genre") {
        const result = await reviewGenreSpotlightContext(ctx, apiKey);
        if (result.fixed) { fixed++; reviewResult = "fixed"; }
        else reviewResult = "no_issues_found";
      } else {
        // No genre context — bust the whole country cache so it regenerates cleanly
        await bustCountryCache(ctx.country);
        reviewResult = "cache_busted";
      }
    } catch (err) {
      console.error(`[flag-review] Error reviewing "${label}":`, err.message);
      reviewResult = "error";
    }

    // Mark all flags in this context as reviewed
    const flagIds = ctx.flags.map(f => f.id);
    await supabase
      .from("track_flags")
      .update({ reviewed_at: new Date().toISOString(), review_result: reviewResult })
      .in("id", flagIds);

    reviewed += ctx.flags.length;
    processed++;

    // Pause between contexts — gives Spotify queue time to drain and Claude a breather
    if (processed < Math.min(contexts.size, maxContexts)) {
      await new Promise(r => setTimeout(r, 2500));
    }
  }

  console.log(`[flag-review] Done — ${reviewed} flag(s) reviewed, ${fixed} cache(s) fixed`);
  return { reviewed, fixed };
}

// ── Deezer ID backfill ────────────────────────────────────
// Given a list of artist names, enriches their cached artist-tracks entries
// with deezerId/deezerUrl/previewUrl by title-matching against Deezer's top tracks.
// Runs entirely in the background — safe to fire-and-forget.
async function backfillDeezerForArtists(artistNames) {
  if (!supabase || !artistNames?.length) return;
  const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();

  for (const artistName of artistNames) {
    try {
      // Try both Spotify and Apple Music cache entries for this artist
      const cacheKeys = [
        makeCacheKey(["artist-tracks", artistName]),
        makeCacheKey(["artist-tracks-apple", artistName]),
      ];

      const { data: rows } = await supabase
        .from("recommendation_cache")
        .select("cache_key, result")
        .in("cache_key", cacheKeys);

      if (!rows?.length) continue;

      // Fetch Deezer tracks once, reuse for both entries
      let deezerTracks = null;

      for (const row of rows) {
        const tracks = row.result?.tracks;
        if (!Array.isArray(tracks) || tracks.length === 0) continue;
        if (tracks.every(t => t.deezerId)) continue;

        if (!deezerTracks) {
          deezerTracks = await deezerArtistTopTracks(artistName, 10);
          if (deezerTracks.length === 0) break;
        }

        const deezerByTitle = new Map(deezerTracks.map(t => [normalise(t.title), t]));
        let changed = false;
        const merged = tracks.map(t => {
          if (t.deezerId) return t;
          const match = deezerByTitle.get(normalise(t.title));
          if (!match) return t;
          changed = true;
          return { ...t, deezerId: match.deezerId, deezerUrl: match.deezerUrl, previewUrl: t.previewUrl || match.previewUrl };
        });

        if (!changed) continue;

        await supabase
          .from("recommendation_cache")
          .update({ result: { ...row.result, tracks: merged } })
          .eq("cache_key", row.cache_key);

        console.log(`[deezer-backfill] enriched ${artistName} (${row.cache_key})`);
      }
    } catch (e) {
      console.warn(`[deezer-backfill] failed for ${artistName}:`, e.message);
    }
    await new Promise(r => setTimeout(r, 200));
  }
}

// ── Enrichment queue ──────────────────────────────────────
async function addToEnrichmentQueue(artists, country) {
  if (!supabase || !artists?.length) return;
  const rows = artists.map(a => ({ country, artist: a.name }));
  await supabase.from("enrichment_queue")
    .upsert(rows, { onConflict: "country,artist", ignoreDuplicates: true });
}

async function processEnrichmentBatch(batchSize = 5) {
  if (!supabase) return { processed: 0, total: 0 };

  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const { data: items } = await supabase
    .from("enrichment_queue")
    .select("id, country, artist, attempts")
    .is("completed_at", null)
    .lt("attempts", 3)
    .or(`last_attempted_at.is.null,last_attempted_at.lt.${oneHourAgo}`)
    .order("created_at", { ascending: true })
    .limit(batchSize);

  if (!items?.length) return { processed: 0, total: 0 };

  const appleToken = generateAppleMusicToken();
  let processed = 0;
  for (const item of items) {
    await supabase.from("enrichment_queue")
      .update({ attempts: item.attempts + 1, last_attempted_at: new Date().toISOString() })
      .eq("id", item.id);

    try {
      // Last.fm top tracks give us seed titles to search with — much better hit rate
      const lfTitles = await lastfmArtistTopTracks(item.artist, 5);
      if (lfTitles.length > 0) console.log(`[enrich] Last.fm → ${lfTitles.length} seed titles for "${item.artist}"`);

      const timeout = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('timeout')), 20000)
      );
      // Use Apple Music + LB enriched with Last.fm titles; fall back to Spotify queue
      const trackPromise = lfTitles.length > 0
        ? proactiveArtistTracks(item.artist, lfTitles, appleToken)
        : fetchArtistTracks(item.artist);
      const tracks = await Promise.race([trackPromise, timeout]);

      if (tracks.length > 0) {
        const cacheKey = makeCacheKey(["artist-tracks-apple", item.artist]);
        artistTracksMemCache.set(cacheKey, { tracks, cachedAt: Date.now() });
        storeCache(cacheKey, "artist-tracks-apple", { tracks }).catch(() => {});
        await supabase.from("enrichment_queue")
          .update({ completed_at: new Date().toISOString() })
          .eq("id", item.id);
        console.log(`[enrich] ✓ ${item.artist} (${item.country}) → ${tracks.length} tracks`);
        processed++;
      } else {
        console.log(`[enrich] – ${item.artist} (${item.country}) → no tracks found`);
      }
    } catch (err) {
      console.error(`[enrich] ✗ ${item.artist}: ${err.message}`);
    }
  }

  return { processed, total: items.length };
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

// Pick n artists from pool ensuring era diversity across decades.
function pickDiverseByEra(arr, n) {
  // Prefer artists with verified tracks; fill remaining slots from unverified
  const withTracks = arr.filter(a => a.hasVerifiedTracks);
  const withoutTracks = arr.filter(a => !a.hasVerifiedTracks);

  function pickByEra(pool, limit) {
    const shuffled = [...pool].sort(() => Math.random() - 0.5);
    const byEra = {};
    for (const a of shuffled) {
      const era = a.era || "Other";
      (byEra[era] = byEra[era] || []).push(a);
    }
    const eras = Object.keys(byEra).sort(() => Math.random() - 0.5);
    const chosen = [];
    while (chosen.length < limit) {
      let added = false;
      for (const era of eras) {
        if (chosen.length >= limit) break;
        if (byEra[era].length === 0) continue;
        chosen.push(byEra[era].shift());
        added = true;
      }
      if (!added) break;
    }
    return chosen;
  }

  const fromVerified = pickByEra(withTracks, n);
  const remaining = n - fromVerified.length;
  const fromUnverified = remaining > 0 ? pickByEra(withoutTracks, remaining) : [];
  // Verified artists come first in the returned list
  return [...fromVerified, ...fromUnverified];
}

// Annotate each artist with hasVerifiedTracks based on in-memory track cache
function annotateTrackStatus(artists) {
  return artists.map(a => {
    const ck = makeCacheKey(["artist-tracks-apple", a.name]);
    const mem = artistTracksMemCache.get(ck);
    const hasVerifiedTracks = !!(mem && mem.tracks.length > 0 && Date.now() - mem.cachedAt < ARTIST_TRACKS_TTL_MS);
    return { ...a, hasVerifiedTracks };
  });
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
        console.log(`[Discogs] Master: "${masterData.title}" by ${releaseArtist} (id:${master.id}, year:${masterData.year})`);
        for (const track of (masterData.tracklist || []).slice(0, 4)) {
          if (!track.title || track.type_ === "heading") continue;
          // Some tracks have their own artist credits
          const trackArtist = track.artists?.length
            ? track.artists.map(a => a.name).join(", ")
            : releaseArtist;
          tracks.push({ title: track.title, artist: trackArtist });
          console.log(`[Discogs]   → "${track.title}" by ${trackArtist}`);
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
const MB_UA   = "MusicalPassport/1.0 (cellorepertoireinitiative@gmail.com)";

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

  // Check dedicated artist_countries table (permanent reference data, no TTL)
  if (supabase) {
    const slug = makeCacheKey([key]);
    const { data } = await supabase
      .from("artist_countries")
      .select("country_code")
      .eq("artist_slug", slug)
      .single();
    if (data) {
      mbArtistCache.set(key, { country: data.country_code, at: Date.now() });
      return data.country_code;
    }
  }
  return undefined; // cache miss
}

async function setMbArtistCached(artistName, country) {
  const key = artistName.toLowerCase().trim();
  mbArtistCache.set(key, { country, at: Date.now() });
  if (supabase) {
    const slug = makeCacheKey([key]);
    await supabase.from("artist_countries").upsert(
      { artist_slug: slug, country_code: country, source: "musicbrainz" },
      { onConflict: "artist_slug" }
    );
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

// ── Streaming floor helpers ───────────────────────────────
const DECADES_LIST = ["1900s","1910s","1920s","1930s","1940s","1950s","1960s","1970s","1980s","1990s","2000s","2010s","2020s"];
function yearToDecade(year) {
  const y = parseInt(year);
  if (isNaN(y) || y < 1900) return null;
  return `${Math.floor(y / 10) * 10}s`;
}
function decadeIndex(decade) { return DECADES_LIST.indexOf(decade); }

// Patch streamingFloor in-place without touching other cache fields (preserves expires_at, cached_at, etc.)
async function patchStreamingFloor(country, floor) {
  if (!supabase || !country || !floor) return;
  const cacheKey = makeCacheKey(["recommend", country]);
  const { data } = await supabase.from("recommendation_cache").select("result").eq("cache_key", cacheKey).single();
  if (!data?.result) return;
  if (data.result.streamingFloor === floor) return; // already correct
  await supabase.from("recommendation_cache")
    .update({ result: { ...data.result, streamingFloor: floor } })
    .eq("cache_key", cacheKey);
  console.log(`[streaming-floor] ${country}: floor set to ${floor}`);
}

// Called after getting validated Spotify/Apple tracks — improves floor if a track predates it
async function maybeImproveStreamingFloor(country, tracks) {
  if (!supabase || !country || !tracks?.length) return;
  const cacheKey = makeCacheKey(["recommend", country]);
  const { data } = await supabase.from("recommendation_cache").select("result").eq("cache_key", cacheKey).single();
  if (!data?.result?.streamingFloor) return;
  const currentIdx = decadeIndex(data.result.streamingFloor);
  if (currentIdx <= 0) return; // already at earliest
  let bestIdx = currentIdx;
  for (const t of tracks) {
    const year = t.releaseYear || t.year;
    if (!year) continue;
    const decade = yearToDecade(year);
    const idx = decadeIndex(decade);
    if (idx >= 0 && idx < bestIdx) bestIdx = idx;
  }
  if (bestIdx < currentIdx) {
    const improved = DECADES_LIST[bestIdx];
    await supabase.from("recommendation_cache")
      .update({ result: { ...data.result, streamingFloor: improved } })
      .eq("cache_key", cacheKey);
    console.log(`[streaming-floor] ${country}: floor improved ${data.result.streamingFloor} → ${improved}`);
  }
}

// Reverse map: ISO-2 → canonical country name (built once from COUNTRY_ISO)
const ISO_TO_COUNTRY = Object.fromEntries(
  Object.entries(COUNTRY_ISO)
    .filter(([name]) => !["Wales","Scotland"].includes(name)) // keep canonical UK names
    .map(([name, code]) => [code, name])
);
// Patch a few names MusicBrainz uses that differ from our display names
ISO_TO_COUNTRY["US"] = "United States";
ISO_TO_COUNTRY["GB"] = "United Kingdom";
ISO_TO_COUNTRY["KR"] = "South Korea";
ISO_TO_COUNTRY["TW"] = "Taiwan";
ISO_TO_COUNTRY["VN"] = "Vietnam";
ISO_TO_COUNTRY["IR"] = "Iran";
ISO_TO_COUNTRY["CI"] = "Ivory Coast";

// Verify and correct country info for each artist in a pool using MusicBrainz.
// Runs lookups in the existing MB queue (rate-limited to 1/sec), so it's slow but accurate.
async function verifyPoolCountries(pool) {
  return Promise.all(pool.map(async (artist) => {
    const mbCode = await mbArtistCountry(artist.name);
    if (!mbCode || mbCode === artist.countryCode) return artist; // no change needed
    const mbName = ISO_TO_COUNTRY[mbCode];
    if (!mbName) return artist; // unknown ISO code, keep Claude's answer
    console.log(`[verify-country] ${artist.name}: ${artist.countryCode}(${artist.country}) → ${mbCode}(${mbName})`);
    return { ...artist, country: mbName, countryCode: mbCode };
  }));
}

// Parse "1960s" → { start: 1960, end: 1969 }
function parseDecade(decade) {
  const yr = parseInt(decade, 10);
  if (isNaN(yr)) return null;
  return { start: yr, end: yr + 9 };
}

// Look up an artist on MusicBrainz; returns their ISO country code or null
async function mbArtistCountry(artistName) {
  const cached = await getMbArtistCached(artistName);
  if (cached !== undefined) return cached?.trim() ?? null;
  try {
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(artistName)}&limit=3&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) { await setMbArtistCached(artistName, null); return null; }
    const d = await r.json();
    const top = (d.artists || []).find(a => (a.score || 0) >= 80);
    const country = top?.country?.trim() ?? null;
    await setMbArtistCached(artistName, country);
    return country;
  } catch { return null; }
}

// ── ListenBrainz fallback for artist tracks ───────────────
const LB_BASE = "https://api.listenbrainz.org/1";
const LB_TIMEOUT_MS = 8000;

async function lbFetch(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), LB_TIMEOUT_MS);
  try {
    const r = await fetch(url, {
      ...options,
      headers: { "User-Agent": MB_UA, ...(options.headers || {}) },
      signal: controller.signal,
    });
    return r;
  } catch (e) {
    if (e.name === "AbortError") {
      console.warn(`[LB] Request timed out: ${url}`);
      return { ok: false, status: 408 };
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// ── LB popular-recordings-by-country ─────────────────────
const areaMbidCache = new Map(); // countryName → { mbid, at }
const AREA_MBID_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days

async function getAreaMBID(countryName) {
  const cached = areaMbidCache.get(countryName);
  if (cached && Date.now() - cached.at < AREA_MBID_CACHE_TTL) return cached.mbid;
  try {
    const url = `${MB_BASE}/area?query=area:${encodeURIComponent(countryName)}&limit=5&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) { areaMbidCache.set(countryName, { mbid: null, at: Date.now() }); return null; }
    const d = await r.json();
    const areas = d.areas || [];
    const top = areas.find(a => (a.score || 0) >= 80 && a.type === "Country")
             ?? areas.find(a => (a.score || 0) >= 80);
    const mbid = top?.id ?? null;
    areaMbidCache.set(countryName, { mbid, at: Date.now() });
    return mbid;
  } catch { return null; }
}

async function lbPopularRecordingsByCountry(countryName, decade) {
  const areaMbid = await getAreaMBID(countryName);
  if (!areaMbid) {
    console.log(`[LB] No area MBID found for "${countryName}"`);
    return [];
  }
  const range = parseDecade(decade);
  if (!range) return [];
  try {
    const r = await lbFetch("https://datasets.listenbrainz.org/popular-recordings-by-country/json", {
      method: "POST",
      body: JSON.stringify([{ "[area_mbid]": areaMbid }]),
      headers: { "Content-Type": "application/json" },
    });
    if (!r.ok) {
      console.warn(`[LB] popular-recordings-by-country failed for "${countryName}": ${r.status}`);
      return [];
    }
    const data = await r.json();
    const tracks = (Array.isArray(data) ? data : [])
      .filter(rec => rec.year && rec.year >= range.start && rec.year <= range.end)
      .sort((a, b) => (b.listen_count || 0) - (a.listen_count || 0))
      .slice(0, 30)
      .map(rec => ({ title: rec.recording_name, artist: rec.artist_credit_name, year: rec.year }));
    console.log(`[LB] Got ${tracks.length} popular recordings for "${countryName}" ${decade} (area: ${areaMbid})`);
    return tracks;
  } catch (err) {
    console.error(`[LB] popular-recordings-by-country error for "${countryName}":`, err.message);
    return [];
  }
}

// Returns unique artists for a country from LB (no decade filter), sorted by total listen count.
async function lbTopArtistsForCountry(countryName) {
  const areaMbid = await getAreaMBID(countryName);
  if (!areaMbid) return [];
  try {
    const r = await lbFetch("https://datasets.listenbrainz.org/popular-recordings-by-country/json", {
      method: "POST",
      body: JSON.stringify([{ "[area_mbid]": areaMbid }]),
      headers: { "Content-Type": "application/json" },
    });
    if (!r.ok) return [];
    const data = await r.json();
    // Aggregate listen counts per artist
    const artistMap = new Map();
    for (const rec of Array.isArray(data) ? data : []) {
      const name = rec.artist_credit_name;
      if (!name) continue;
      const prev = artistMap.get(name) || { name, listenCount: 0 };
      prev.listenCount += rec.listen_count || 0;
      artistMap.set(name, prev);
    }
    const artists = [...artistMap.values()]
      .sort((a, b) => b.listenCount - a.listenCount)
      .slice(0, 40);
    console.log(`[LB] ${artists.length} unique artists for "${countryName}"`);
    return artists;
  } catch (err) {
    console.error(`[LB] lbTopArtistsForCountry error for "${countryName}":`, err.message);
    return [];
  }
}

// Returns artists MB has linked to a country/area, with genre tags for context.
async function mbArtistsByCountry(country, isoCode) {
  try {
    const url = `${MB_BASE}/artist?query=area:${encodeURIComponent(country)}&limit=50&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) return [];
    const d = await r.json();
    return (d.artists || [])
      .filter(a => (a.score || 0) >= 70 && (!a.country || a.country === isoCode))
      .map(a => ({
        name: a.name,
        country: a.country || null,
        tags: (a.tags || []).map(t => t.name).slice(0, 5),
      }));
  } catch (err) {
    console.error(`[MB] mbArtistsByCountry error for "${country}":`, err.message);
    return [];
  }
}

// Cross-references LB and MB to build a pool of verified real artists for a country.
// Artists in BOTH sources = high confidence. Single source = medium confidence.
// Returns up to 20 artists sorted by confidence then listen count.
async function buildRealArtistPool(country, isoCode) {
  const [lbArtists, mbArtists] = await Promise.all([
    lbTopArtistsForCountry(country),
    mbArtistsByCountry(country, isoCode),
  ]);

  const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  const mbByNorm = new Map(mbArtists.map(a => [norm(a.name), a]));
  const lbByNorm = new Map(lbArtists.map(a => [norm(a.name), a]));

  const seen = new Set();
  const pool = [];

  // High confidence: artists in both LB and MB
  for (const lbArtist of lbArtists) {
    const key = norm(lbArtist.name);
    if (seen.has(key)) continue;
    if (mbByNorm.has(key)) {
      seen.add(key);
      pool.push({ name: lbArtist.name, confidence: "high", listenCount: lbArtist.listenCount, tags: mbByNorm.get(key).tags });
    }
  }
  // Medium confidence: LB only
  for (const lbArtist of lbArtists) {
    const key = norm(lbArtist.name);
    if (seen.has(key)) continue;
    seen.add(key);
    pool.push({ name: lbArtist.name, confidence: "medium-lb", listenCount: lbArtist.listenCount, tags: [] });
  }
  // Medium confidence: MB only
  for (const mbArtist of mbArtists) {
    const key = norm(mbArtist.name);
    if (seen.has(key)) continue;
    seen.add(key);
    pool.push({ name: mbArtist.name, confidence: "medium-mb", listenCount: 0, tags: mbArtist.tags });
  }

  const result = pool.slice(0, 20);
  console.log(`[real-pool] ${country}: ${result.filter(a => a.confidence === "high").length} high-conf, ${result.filter(a => a.confidence !== "high").length} medium-conf artists`);
  return result;
}

// Simple in-memory cache for artist MBIDs
const mbidCache = new Map(); // artistName → { mbid, at }
const MBID_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days

async function getMbArtistMBID(artistName) {
  const cached = mbidCache.get(artistName);
  if (cached && Date.now() - cached.at < MBID_CACHE_TTL) return cached.mbid;
  try {
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(artistName)}&limit=3&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) { mbidCache.set(artistName, { mbid: null, at: Date.now() }); return null; }
    const d = await r.json();
    const top = (d.artists || []).find(a => (a.score || 0) >= 80);
    const mbid = top?.id ?? null;
    mbidCache.set(artistName, { mbid, at: Date.now() });
    return mbid;
  } catch { return null; }
}

async function fetchArtistTracksFromLB(artistName) {
  try {
    const mbid = await getMbArtistMBID(artistName);
    if (!mbid) {
      console.log(`  [LB] No MBID found for "${artistName}"`);
      return [];
    }
    const r = await lbFetch(`${LB_BASE}/popularity/top-recordings-for-artist/${mbid}`);
    if (!r.ok) {
      console.warn(`  [LB] popularity fetch failed for "${artistName}": ${r.status}`);
      return [];
    }
    const data = await r.json();
    const recordings = Array.isArray(data) ? data : (data.recordings || []);
    const tracks = recordings.slice(0, 3).map(rec => ({
      title: rec.recording_name,
      artist: rec.artist_name,
    }));
    // Validate that the returned tracks actually belong to the requested artist.
    // MusicBrainz fuzzy search can match the wrong MBID (e.g. "George Boe" → Alfie Boe).
    const normalise = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const target = normalise(artistName);
    const anyMatch = tracks.some(t => {
      const n = normalise(t.artist || "");
      return n.includes(target) || (n.length >= 6 && target.includes(n));
    });
    if (tracks.length > 0 && !anyMatch) {
      console.log(`  [LB] Rejected "${artistName}" tracks — artist mismatch (got: ${tracks[0]?.artist})`);
      return [];
    }
    console.log(`  [LB] Got ${tracks.length} tracks for "${artistName}" via ListenBrainz`);
    return tracks;
  } catch (err) {
    console.error(`  [LB] Error fetching tracks for "${artistName}":`, err.message);
    return [];
  }
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

app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/api/spotify-status", (_req, res) => {
  const now = Date.now();
  if (now >= spotifyRateLimitedUntil) {
    return res.json({ rateLimited: false, queueLength: spotifyQueue.length });
  }
  const remainingMs = spotifyRateLimitedUntil - now;
  const remainingS = Math.ceil(remainingMs / 1000);
  const unlocksAt = new Date(spotifyRateLimitedUntil).toISOString();
  return res.json({
    rateLimited: true,
    remainingSeconds: remainingS,
    remainingHuman: `${Math.floor(remainingS / 3600)}h ${Math.floor((remainingS % 3600) / 60)}m ${remainingS % 60}s`,
    unlocksAt,
    queueLength: spotifyQueue.length,
  });
});

// In-flight request coalescing — prevents cache stampedes when two requests
// for the same country arrive simultaneously before either has been cached.
const recommendInFlight = new Map(); // cacheKey → Promise

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

  // Single shared cache per country — recommendations are not personalized
  const cacheKey = makeCacheKey(["recommend", country]);
  const cached = await getCached(cacheKey);
  if (cached && cached.artist_pool && cached.artist_pool.length >= 4) {
    console.log(`[recommend] cache hit → ${country}`);
    const pool = await filterOutFlaggedArtists(cached.artist_pool);
    if (pool.length >= 4) {
      return res.json({
        genres: cached.result.genres,
        artists: pickDiverseByEra(annotateTrackStatus(pool), 4),
        didYouKnow: cached.result.didYouKnow,
      });
    }
    console.log(`[recommend] pool shrank to ${pool.length} after filtering flagged — regenerating`);
  }

  // Coalescing: if another request for this country is already in-flight,
  // piggyback on it instead of launching a duplicate Claude pipeline.
  const _inflightKey = cacheKey;
  let _resolveInFlight = null;
  let _rejectInFlight = null;
  if (recommendInFlight.has(_inflightKey)) {
    console.log(`[recommend] coalescing → ${country}`);
    try {
      return res.json(await recommendInFlight.get(_inflightKey));
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }
  const inflightPromise = new Promise((resolve, reject) => {
    _resolveInFlight = resolve;
    _rejectInFlight = reject;
  });
  recommendInFlight.set(_inflightKey, inflightPromise);

  // Kick off real-data pool lookup
  const isoCode = COUNTRY_ISO[country];
  const realPoolPromise = isoCode ? buildRealArtistPool(country, isoCode) : Promise.resolve([]);

  try {
    const realPool = await realPoolPromise;
    const realPoolNote = realPool.length > 0
      ? `\n\nVERIFIED ARTISTS from MusicBrainz + ListenBrainz databases for ${country}:\n${
          realPool.map(a => `- ${a.name}${a.confidence === "high" ? " [verified in both MB+LB]" : ""}${a.tags.length ? ` (${a.tags.slice(0, 3).join(", ")})` : ""}`).join("\n")
        }\n\nThese artists are confirmed to be from ${country} by music databases. Your 12 artists MUST include as many of these as fit the era/genre mix. You may add artists NOT in this list only if you are certain they are from ${country} — and you must include their name exactly as known. Do NOT invent or misattribute artists.`
      : "";

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
- The "didYouKnow" should reveal something genuinely surprising about that civilization's music.

Return exactly this JSON:
{
  "genres": ["genre1","genre2","genre3"],
  "artists": [
    {
      "name": "Name",
      "genre": "specific genre",
      "era": "1980s",
      "similarTo": "One well-known artist name only, no description (e.g. 'Bob Dylan')"
    }
  ],
  "didYouKnow": "One surprising musical fact about ${country}"
}
era must be a decade string — exactly one of: 1900s, 1910s, 1920s, 1930s, 1940s, 1950s, 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s — representing the decade this artist was most active or is most associated with. Include exactly 12 artists with a varied spread of decades — include artists from at least 3 different decade groups.
IMPORTANT: Use each artist's exact real name as it appears on streaming platforms. Do NOT repeat a name (e.g. write "Banah" not "Banah Banah") unless the repeated form is the actual official band name (e.g. "Duran Duran", "Talk Talk" are correct).${realPoolNote}`,
          },
        ],
      }),
    });

    if (response.status === 529) {
      return res.status(503).json({ error: "Our servers are busy right now. Try again in a moment." });
    }

    const data = await response.json();

    if (data.error) {
      return res.status(500).json({ error: data.error.message });
    }

    const raw = (data.content[0].text || "")
      .replace(/```json|```/g, "")
      .trim();
    let rec;
    try { rec = JSON.parse(raw); } catch (parseErr) {
      console.error(`[recommend] JSON.parse failed for ${country}:`, parseErr.message, "\nRaw:", raw.slice(0, 200));
      throw parseErr;
    }
    rec.artists = normalizeArtistNames(rec.artists);
    rec.genres = await Promise.all((rec.genres || []).map(g => resolveGenreCanonical(g)));

    console.log(`[recommend] Claude → ${country} (${rec.artists.length} artists, ${rec.genres.length} genres)`);

    // Strip artists already known to be unplayable before verification
    const unflaggedArtists = await filterOutFlaggedArtists(rec.artists);
    if (unflaggedArtists.length < rec.artists.length) {
      console.log(`[recommend] removed ${rec.artists.length - unflaggedArtists.length} pre-flagged artists for ${country}`);
    }

    // Verify remaining artists have playable tracks before storing
    const verifiedPool = await verifyArtistTracksForRecommend(unflaggedArtists, country);
    console.log(`[recommend] verified ${verifiedPool.length}/${rec.artists.length} artists have tracks for ${country}`);

    // Use verified pool if large enough, otherwise fall back to full Claude pool
    // (better to show something than nothing while cron fixes it)
    const verifiedNames = new Set(verifiedPool.map(a => a.name));
    const rawPool = verifiedPool.length >= 4 ? verifiedPool : rec.artists;
    // Annotate each artist with whether we confirmed they have tracks
    const artistPoolBase = rawPool.map(a => ({ ...a, hasVerifiedTracks: verifiedNames.has(a.name) }));

    // Fetch artist image URLs (Spotify → Last.fm fallback) — fire and forget errors
    const imageUrls = await Promise.all(artistPoolBase.map(a => fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null)));
    const artistPool = artistPoolBase.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || undefined }));

    await storeCache(cacheKey, "recommend", { genres: rec.genres, didYouKnow: rec.didYouKnow }, artistPool);
    verifyArtistPoolWithMB(artistPool, country, [cacheKey]).catch(() => {});

    const _result = { genres: rec.genres, artists: pickDiverseByEra(artistPool, 4), didYouKnow: rec.didYouKnow };
    if (_resolveInFlight) { _resolveInFlight(_result); recommendInFlight.delete(_inflightKey); }
    res.json(_result);

    // Queue any unverified artists for deep enrich in background
    const unverified = rec.artists.filter(a => !verifiedPool.find(v => v.name === a.name));
    if (unverified.length) addToEnrichmentQueue(unverified, country).catch(() => {});

    // Backfill Deezer IDs for all artists in this country's pool
    backfillDeezerForArtists(artistPool.map(a => a.name)).catch(() => {});
  } catch (err) {
    if (_rejectInFlight) { _rejectInFlight(err); recommendInFlight.delete(_inflightKey); }
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
      messages: [{ role: "user", content: `These recordings were found in music databases for ${country} in the ${decade}. Many are FALSE POSITIVES — international releases that were pressed or distributed in ${country} but are by artists from other countries.\n\n${trackList}\n\nReturn JSON: {"genre": "the dominant genre or style (be specific, name the actual local genre)", "picks": ["track title 1", "track title 2", ...]} — pick ONLY tracks by artists who were BORN IN or FORMED IN ${country}. Rules:\n1. HARD REJECT: any artist you know to be from a different country, even if they were popular in ${country}. When in doubt, reject.\n2. MAX 2 tracks per artist — the picks must span multiple different artists.\n3. Return only what you are confident about. If you cannot find 3+ authentic local artists in this list, return an empty picks array rather than guessing.` }],
    }),
  });
  const genreData = await genreResponse.json();
  if (genreData.error) return null;

  const genreRaw = (genreData.content[0].text || "").replace(/```json|```/g, "").trim();
  const genreParsed = JSON.parse(genreRaw);
  const pickedTitles = new Set((genreParsed.picks || []).map(t => t.toLowerCase()));
  const pickedTracksRaw = sourceTracks.filter(t => pickedTitles.has(t.title.toLowerCase()));
  // Enforce max 2 tracks per artist
  const artistCount = new Map();
  const pickedTracks = pickedTracksRaw.filter(t => {
    const key = t.artist.toLowerCase();
    const count = artistCount.get(key) || 0;
    if (count >= 2) return false;
    artistCount.set(key, count + 1);
    return true;
  }).slice(0, 8);
  // Always use Claude's picks — never fall back to raw sourceTracks which has no artist diversity.
  // If Claude found zero authentic picks, there's nothing valid to search.
  const tracksToSearch = pickedTracks;

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
    const appleArtistCount = new Map();
    validTracks = results.filter(t => t.appleId).filter(t => {
      const key = t.artist.toLowerCase();
      const count = appleArtistCount.get(key) || 0;
      if (count >= 2) return false;
      appleArtistCount.set(key, count + 1);
      return true;
    }).slice(0, 5);
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
    const spotifyArtistCount = new Map();
    validTracks = results.filter(t => t.spotifyId).filter(t => {
      const key = t.artist.toLowerCase();
      const count = spotifyArtistCount.get(key) || 0;
      if (count >= 2) return false;
      spotifyArtistCount.set(key, count + 1);
      return true;
    }).slice(0, 5);
  }

  const missedTracks = tracksToSearch.filter(t => {
    return !validTracks.find(v => v.title === t.title && v.artist === t.artist);
  });
  if (missedTracks.length > 0) {
    console.log(`[${source}] Not found on ${service}: ${missedTracks.map(t => `"${t.title}" by ${t.artist}`).join(", ")}`);
  }
  if (validTracks.length >= 3) {
    return { genre: genreParsed.genre, tracks: validTracks, source };
  }

  // Streaming verification failed but Claude authenticated real picks from a trusted source.
  // Return tracks without streaming IDs — the mobile app renders them with a disabled play button.
  if (pickedTracks.length >= 3) {
    console.log(`[${source}] Streaming not available for ${pickedTracks.length} authentic tracks — returning without streaming IDs`);
    const unverifiedTracks = pickedTracks.map(t => ({
      ...t,
      appleId: null,
      spotifyId: null,
      previewUrl: null,
      spotifyUrl: null,
      embedUrl: null,
    }));
    return { genre: genreParsed.genre, tracks: unverifiedTracks, source };
  }

  return null;
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
  if (tmCached) {
    console.log(`[time-machine] cache hit (${tmCached.result.source ?? "claude"}) → ${country} ${decade}`);
    return res.json({ country, decade, ...tmCached.result });
  }

  const isoCode = COUNTRY_ISO[country];

  try {
    // ── 1. ListenBrainz popular-recordings-by-country (primary) ──
    // Real listen-count ranked data with year field for accurate decade filtering.
    console.log(`[LB] Fetching popular recordings for ${country} ${decade}`);
    const lbTracks = await lbPopularRecordingsByCountry(country, decade);
    if (lbTracks.length >= 5) {
      const lbFiltered = isoCode ? await filterTracksByArtistOrigin(lbTracks, isoCode) : lbTracks;
      console.log(`[LB] ${lbTracks.length} → ${lbFiltered.length} after origin filter`);
      const pool = lbFiltered.length >= 5 ? lbFiltered : lbTracks;
      const resolved = await resolveRealTracks(pool, country, decade, service, apiKey, "listenbrainz");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[LB] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
      console.log(`[LB] Not enough tracks found on streaming, trying MusicBrainz`);
    }

    // ── 2. MusicBrainz recordings (strong for historical, fills LB gaps) ──
    // MB queries by release country, which returns Western albums pressed on local labels —
    // always filter by artist origin before using results anywhere (including the merged pool).
    let mbTracks = [];
    let mbFiltered = [];
    if (isoCode) {
      console.log(`[MB] Fetching recordings for ${country} (${isoCode}) ${decade}`);
      mbTracks = await mbRecordingsForCountryDecade(isoCode, decade);
      console.log(`[MB] Got ${mbTracks.length} recordings`);
      if (mbTracks.length > 0) {
        mbFiltered = await filterTracksByArtistOrigin(mbTracks, isoCode);
        console.log(`[MB] ${mbTracks.length} → ${mbFiltered.length} after origin filter`);
      }
    }
    if (mbFiltered.length >= 5) {
      const resolved = await resolveRealTracks(mbFiltered, country, decade, service, apiKey, "musicbrainz");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[MB] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
      console.log(`[MB] Not enough tracks found on streaming, trying Discogs`);
    }

    // ── 3. Discogs (better coverage for non-Western and obscure releases) ──
    const discogsTracks = await discogsTracksForCountryDecade(country, decade);
    console.log(`[Discogs] Got ${discogsTracks.length} tracks for ${country} ${decade}`);
    if (discogsTracks.length >= 5) {
      const resolved = await resolveRealTracks(discogsTracks, country, decade, service, apiKey, "discogs");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[Discogs] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
      console.log(`[Discogs] Not enough tracks on streaming, trying merged pool`);
    }

    // ── 4. Merge all three sources and try again ──
    // Uses mbFiltered (origin-verified) not raw mbTracks to keep Western false-positives out.
    const seen = new Set();
    const mergedTracks = [...lbTracks, ...mbFiltered, ...discogsTracks].filter(t => {
      const key = `${t.title.toLowerCase()}||${t.artist.toLowerCase()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (mergedTracks.length >= 5) {
      console.log(`[time-machine] Trying merged pool (${mergedTracks.length} tracks) for ${country} ${decade}`);
      const resolved = await resolveRealTracks(mergedTracks, country, decade, service, apiKey, "merged");
      if (resolved) {
        const tmResult = { country, decade, ...resolved };
        await storeCache(tmCacheKey, "time-machine", tmResult);
        console.log(`[merged] Time Machine served ${resolved.tracks.length} tracks for ${country} ${decade}`);
        return res.json(tmResult);
      }
    }

    console.log(`[time-machine] All real sources exhausted for ${country} ${decade}, falling back to Claude`);

    // Build context from all real tracks found — even those that didn't land on streaming.
    // This anchors Claude to artists we know are genuinely from this country/decade.
    const allRealTracks = [...lbTracks, ...mbTracks, ...discogsTracks];
    const seenArtists = new Set();
    const knownArtists = allRealTracks
      .filter(t => { if (seenArtists.has(t.artist)) return false; seenArtists.add(t.artist); return true; })
      .slice(0, 15)
      .map(t => `- ${t.artist} ("${t.title}")`);
    const knownArtistContext = knownArtists.length > 0
      ? `\n\nKNOWN REAL ARTISTS FROM ${country.toUpperCase()} IN THE ${decade} (verified from MusicBrainz, ListenBrainz, and Discogs):\n${knownArtists.join("\n")}\nPrioritize these artists. You may supplement with additional artists you know to be genuinely from ${country}, but every artist must be from ${country} or its predecessor region.`
      : "";

    // ── Fallback: Claude picks all tracks ──
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
        system: "You are a world music historian and ethnomusicologist with encyclopedic knowledge of global music across all eras. You MUST always return valid JSON — never refuse, never add preamble. If a country/decade combination has very sparse recorded output, use field recordings, modern revival artists, or diaspora artists — but always return the JSON structure.",
        messages: [{
          role: "user",
          content: `Spotlight a music genre connecting "${country}" and the "${decade}".

CRITICAL AUTHENTICITY RULE: Every track must be by an artist genuinely FROM or rooted in ${country} (or the predecessor region if the country didn't exist yet). Never recommend Western or globally popular music simply because it was popular in ${country} during that period — that is not authentic local music.

HANDLING SPARSE RECORDING HISTORY (very common before the 1960s):
If ${country} had little formal recorded output in the ${decade}, do NOT substitute Western music. Instead, choose one of these honest approaches:
- Recommend traditional or folk music from that era — including ethnomusicological field recordings made of that tradition, even if recorded later
- Recommend artists active in the region during that period whose recordings survive
- Recommend modern artists from ${country} who authentically revive or perform music from that era
Never fill the gap with internationally popular artists who happened to be famous globally in that decade.

HANDLING COUNTRY EXISTENCE ISSUES:
- Country didn't yet exist in the ${decade} (e.g. Zimbabwe didn't exist until 1980): use the predecessor territory's people and musical culture of that period. Be historically precise.
- Country ceased to exist before the ${decade} (e.g. Rhodesia + 2000s, Rhodesia became Zimbabwe in 1980): spotlight the living successor tradition, diaspora artists, or revival recordings honestly.
- Country existed but had almost no recording industry: apply the sparse recording history rules above.

HISTORICAL/DEFUNCT CIVILIZATIONS (Byzantine Empire, Ottoman Empire, Ancient Rome, etc.):
- If the decade falls within that civilization's existence → authentic music of that era.
- If the decade is after the civilization ended → scholarly recordings of that tradition, ethnomusicological field recordings, or modern artists from the successor region who revive that specific tradition. Be geographically and culturally precise — not just any music from the broader region.

Always find a real, honest musical angle rooted in the actual people and culture of ${country}.${knownArtistContext}

Return exactly this JSON:
{
  "genre": "genre name (be specific and evocative)",
  "tracks": [
    { "title": "track title", "artist": "artist name" }
  ]
}
Include exactly 8 tracks. Every artist must be genuinely from or rooted in ${country} or its predecessor region. MAX 2 tracks per artist — the list must span at least 4 different artists. Prioritize discoverability on major streaming platforms.`,
        }, {
          role: "assistant",
          content: "{",
        }],
      }),
    });

    if (response.status === 529) {
      return res.status(503).json({ error: "Our servers are busy right now. Try again in a moment." });
    }

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const rawText = (data.content[0].text || "").replace(/```json|```/g, "").trim();
    const raw = rawText.startsWith("{") ? rawText : "{" + rawText;
    let spotlight;
    try {
      spotlight = JSON.parse(raw);
    } catch {
      console.error(`[time-machine] Claude returned non-JSON for ${country} ${decade}:`, rawText.slice(0, 200));
      return res.status(500).json({ error: `No music data found for ${country} in the ${decade}.` });
    }

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
      const appleArtistCount2 = new Map();
      validTracks = tracksWithIds.filter(t => t.appleId).filter(t => {
        const k = t.artist.toLowerCase();
        const n = appleArtistCount2.get(k) || 0;
        if (n >= 2) return false;
        appleArtistCount2.set(k, n + 1);
        return true;
      }).slice(0, 5);
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
      const spotifyArtistCount2 = new Map();
      validTracks = tracksWithIds.filter(t => t.spotifyId).filter(t => {
        const k = t.artist.toLowerCase();
        const n = spotifyArtistCount2.get(k) || 0;
        if (n >= 2) return false;
        spotifyArtistCount2.set(k, n + 1);
        return true;
      }).slice(0, 5);
    }

    const tmResult = { country, decade, genre: spotlight.genre, tracks: validTracks };
    await storeCache(tmCacheKey, "time-machine", tmResult);
    console.log(`[time-machine] Claude → ${country} ${decade} (${validTracks.length} tracks)`);
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

  const { genre: rawGenre, country, service = "spotify", relatedArtistNames = [], seedArtist } = req.body;
  if (!rawGenre || !country) return res.status(400).json({ error: "Missing genre or country." });

  const genre = await resolveGenreCanonical(rawGenre);
  if (genre !== rawGenre) console.log(`[genre-spotlight] genre normalised: "${rawGenre}" → "${genre}"`);

  const gsCacheKey = makeCacheKey(["genrespotlight", genre, country, service]);
  let gsCached = await getCached(gsCacheKey);

  // If a seed artist is provided but absent from a sparse cached result, bust the cache so the
  // next request generates a fresh result that explicitly includes them.
  if (gsCached && seedArtist) {
    const cachedTracks = gsCached.result?.tracks || [];
    const normSeed = seedArtist.toLowerCase();
    const hasSeed = cachedTracks.some(t => (t.artist || "").toLowerCase() === normSeed);
    if (!hasSeed && cachedTracks.length < 4) {
      console.log(`[genre-spotlight] cache bypassed — seedArtist "${seedArtist}" absent from sparse result (${cachedTracks.length} tracks)`);
      gsCached = null;
    }
  }

  // Supplement sparse/empty track lists with 1-2 tracks per related artist (runs on cache hits too)
  const supplementFromRelatedArtists = async (result) => {
    if (!relatedArtistNames.length) return result;
    const tracks = [...(result.tracks || [])];
    if (tracks.length >= 4) return result;
    const seenIds = new Set(tracks.map(t => t.appleId || t.deezerId).filter(Boolean));
    const appleToken = generateAppleMusicToken();
    for (const artistName of relatedArtistNames) {
      if (tracks.length >= 5) break;
      try {
        const artistTracks = await proactiveArtistTracks(artistName, [], appleToken);
        const withDeezer = await enrichWithDeezer(artistTracks, artistName);
        let added = 0;
        for (const t of withDeezer) {
          if (added >= 2 || tracks.length >= 5) break;
          const trackId = t.appleId || t.deezerId;
          if (!trackId || seenIds.has(trackId)) continue;
          seenIds.add(trackId); tracks.push(t); added++;
        }
      } catch {}
    }
    if (tracks.length === result.tracks?.length) return result;
    console.log(`[genre-spotlight] supplemented with related artists: ${tracks.length} tracks`);
    return { ...result, tracks, hasLocalScene: tracks.length > 0 ? true : result.hasLocalScene };
  };

  if (gsCached) {
    console.log(`[genre-spotlight] cache hit → ${genre} / ${country}`);
    const supplemented = await supplementFromRelatedArtists(gsCached.result);
    return res.json(supplemented);
  }

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1000,
        system: `You are a world music expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.`,
        messages: [{
          role: "user",
          content: `First assess whether "${genre}" is a NICHE genre (practiced in 1–4 countries with a strong cultural identity, e.g. Malouf, Ma'luf, Gnawa, Byzantine Chant, Gamelan, Morna, Mbube, Jùjú, Tuvan throat singing, Sega, Taarab, Chaabi) or a BROAD genre (practiced worldwide or easily found in many countries as local variants, e.g. Jazz, Rock, Hip-hop, Folk, Classical, Pop, R&B, Electronic, Reggae, Metal).
${seedArtist ? `\nCRITICAL: "${seedArtist}" is a confirmed real artist in this genre. You MUST include them in the artists array and include at least one of their actual, real tracks in the tracks array. Do not invent track titles — only use tracks you are certain exist.\n` : ""}
IF NICHE: the user tapped this genre from a ${country} artist card. Provide globally representative artists and tracks for "${genre}" from any of its home countries worldwide. Set isNicheWorldGenre: true, hasLocalScene: true.

IF BROAD: provide a spotlight on "${genre}" as it exists specifically in "${country}". Every artist and track must genuinely be from ${country}.

Return exactly this JSON:
{
  "isNicheWorldGenre": true,
  "hasLocalScene": true,
  "explanation": "1 sentence — for niche: cultural roots and where it thrives globally; for broad: its history and character in ${country}",
  "tracks": [
    { "title": "exact track title", "artist": "exact artist name" }
  ],
  "artists": ["Artist Name 1", "Artist Name 2", "Artist Name 3"],
  "suggestedGenres": ["Genre 1", "Genre 2", "Genre 3"]
}
- "isNicheWorldGenre": true if niche, false if broad
- "hasLocalScene": for NICHE always true; for BROAD: false if ${country} has fewer than 2 genuine local artists for "${genre}"
- "tracks": for NICHE: 1–6 tracks from key worldwide artists; for BROAD: 1–6 tracks from ${country} only. Return empty array rather than include wrong-country artists. No more than 2 tracks from any single artist — spread across different artists to give a representative sample.
- "artists": for NICHE: up to 6 globally recognized artists; for BROAD: up to 6 from ${country} (empty if hasLocalScene is false)
- "suggestedGenres": for NICHE: 3 related world/niche genres; for BROAD: 3 genres with genuine scenes in ${country}`,
        }],
      }),
    });

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const raw = (data.content[0].text || "").replace(/```json|```/g, "").trim();
    let spotlight;
    try { spotlight = JSON.parse(raw); } catch (parseErr) {
      console.error(`[genre-spotlight] JSON.parse failed (${genre}/${country}):`, parseErr.message, "\nRaw:", raw.slice(0, 200));
      return res.status(500).json({ error: "Failed to parse Claude response" });
    }
    const suggestedArtists = spotlight.artists || [];

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

      // Phase 1: search for each Claude-suggested track directly
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
      tracks = tracksWithIds.filter(t => t.spotifyId);

      // Phase 2: if we have fewer than 4 tracks, search by artist name to fill gaps
      if (tracks.length < 4 && accessToken && suggestedArtists.length > 0) {
        console.log(`[genre-spotlight] only ${tracks.length} tracks found, trying artist-based fallback for ${suggestedArtists.length} artists`);
        const seenIds = new Set(tracks.map(t => t.spotifyId));

        const artistFallbackTracks = await Promise.all(
          suggestedArtists.map(async (artistName) => {
            try {
              // Find the artist on Spotify
              const ar = await fetch(
                `https://api.spotify.com/v1/search?q=${encodeURIComponent(artistName)}&type=artist&limit=5&market=US`,
                { headers: { Authorization: "Bearer " + accessToken } }
              );
              const ad = await ar.json();
              const artist = (ad.artists?.items || []).find(a => artistNamesMatch(artistName, a.name));
              if (!artist) return [];

              // Get their top tracks
              const tr = await fetch(
                `https://api.spotify.com/v1/artists/${artist.id}/top-tracks?market=US`,
                { headers: { Authorization: "Bearer " + accessToken } }
              );
              const td = await tr.json();
              return (td.tracks || []).slice(0, 3).map(t => ({
                title: t.name,
                artist: t.artists?.[0]?.name || artistName,
                spotifyId: t.id,
                previewUrl: t.preview_url || null,
                spotifyUrl: `https://open.spotify.com/track/${t.id}`,
              }));
            } catch { return []; }
          })
        );

        // First pass: 1 track per artist for breadth
        for (const artistTracks of artistFallbackTracks) {
          const t = artistTracks.find(t => !seenIds.has(t.spotifyId));
          if (t) { seenIds.add(t.spotifyId); tracks.push(t); }
          if (tracks.length >= 5) break;
        }
        // Second pass: fill remaining slots with a second track per artist if needed
        if (tracks.length < 4) {
          for (const artistTracks of artistFallbackTracks) {
            const t = artistTracks.find(t => !seenIds.has(t.spotifyId));
            if (t) { seenIds.add(t.spotifyId); tracks.push(t); }
            if (tracks.length >= 5) break;
          }
        }
        console.log(`[genre-spotlight] after artist fallback: ${tracks.length} tracks`);
      }

      // Phase 3: Last.fm tag.getTopTracks — better genre coverage than MusicBrainz for niche genres
      if (tracks.length < 4 && accessToken) {
        try {
          const lfKey = process.env.LASTFM_API_KEY;
          if (lfKey) {
            const lfTag = genre.toLowerCase().replace(/[^a-z0-9\s\-]/g, "").trim();
            const lfRes = await fetch(
              `https://ws.audioscrobbler.com/2.0/?method=tag.getTopTracks&tag=${encodeURIComponent(lfTag)}&limit=20&api_key=${lfKey}&format=json`
            );
            const lfData = await lfRes.json();
            const lfTracks = (lfData.tracks?.track || []).slice(0, 20);
            console.log(`[genre-spotlight] Last.fm tag "${lfTag}" → ${lfTracks.length} candidates`);

            const seenIds = new Set(tracks.map(t => t.spotifyId));
            const lfSpotifyResults = await Promise.all(
              lfTracks.map(async (lt) => {
                const title = lt.name;
                const artist = lt.artist?.name;
                if (!title || !artist) return null;
                try {
                  const sq = `track:${title} artist:${artist}`;
                  const sr = await fetch(
                    `https://api.spotify.com/v1/search?q=${encodeURIComponent(sq)}&type=track&limit=3&market=US`,
                    { headers: { Authorization: "Bearer " + accessToken } }
                  );
                  const sd = await sr.json();
                  const found = (sd.tracks?.items || []).find(t => artistNamesMatch(artist, t.artists?.[0]?.name || ""));
                  if (!found) return null;
                  return {
                    title: found.name,
                    artist: found.artists?.[0]?.name || artist,
                    spotifyId: found.id,
                    previewUrl: found.preview_url || null,
                    spotifyUrl: `https://open.spotify.com/track/${found.id}`,
                  };
                } catch { return null; }
              })
            );

            for (const t of lfSpotifyResults) {
              if (!t || seenIds.has(t.spotifyId)) continue;
              seenIds.add(t.spotifyId);
              tracks.push(t);
              if (tracks.length >= 5) break;
            }
            console.log(`[genre-spotlight] after Last.fm fallback: ${tracks.length} tracks`);
          }
        } catch (lfErr) {
          console.warn(`[genre-spotlight] Last.fm fallback error:`, lfErr.message);
        }
      }

      // Phase 4: MusicBrainz tag search — last resort, skewed toward western music
      if (tracks.length < 4 && accessToken) {
        try {
          const mbTag = genre.toLowerCase().replace(/[^\w\s\-']/g, "").trim();
          const mbUrl = `${MB_BASE}/recording?query=tag:"${encodeURIComponent(mbTag)}"&limit=25&fmt=json`;
          const mbResp = await mbFetch(mbUrl);
          if (mbResp.ok) {
            const mbData = await mbResp.json();
            const mbRecordings = (mbData.recordings || []).slice(0, 15);
            console.log(`[genre-spotlight] MB tag "${mbTag}" → ${mbRecordings.length} candidates`);

            const seenIds = new Set(tracks.map(t => t.spotifyId));
            const isoCode = spotlight.isNicheWorldGenre ? null : (COUNTRY_ISO[country] ?? null);

            // For each MB recording, attempt a Spotify lookup
            const mbSpotifyResults = await Promise.all(
              mbRecordings.map(async (rec) => {
                const recTitle = rec.title;
                const recArtist = rec["artist-credit"]?.[0]?.artist?.name;
                if (!recTitle || !recArtist) return null;

                // If we have an ISO code, filter out MB recordings whose artist
                // country is known and doesn't match — skip ambiguous (null) ones
                const recArtistCountry = rec["artist-credit"]?.[0]?.artist?.country ?? null;
                if (isoCode && recArtistCountry && recArtistCountry !== isoCode) return null;

                try {
                  const sq = `track:${recTitle} artist:${recArtist}`;
                  const sr = await fetch(
                    `https://api.spotify.com/v1/search?q=${encodeURIComponent(sq)}&type=track&limit=3&market=US`,
                    { headers: { Authorization: "Bearer " + accessToken } }
                  );
                  const sd = await sr.json();
                  const found = (sd.tracks?.items || []).find(t => artistNamesMatch(recArtist, t.artists?.[0]?.name || ""));
                  if (!found) return null;
                  return {
                    title: found.name,
                    artist: found.artists?.[0]?.name || recArtist,
                    spotifyId: found.id,
                    previewUrl: found.preview_url || null,
                    spotifyUrl: `https://open.spotify.com/track/${found.id}`,
                  };
                } catch { return null; }
              })
            );

            for (const t of mbSpotifyResults) {
              if (!t || seenIds.has(t.spotifyId)) continue;
              seenIds.add(t.spotifyId);
              tracks.push(t);
              if (tracks.length >= 5) break;
            }
            console.log(`[genre-spotlight] after MB fallback: ${tracks.length} tracks`);
          }
        } catch (mbErr) {
          console.warn(`[genre-spotlight] MB fallback error:`, mbErr.message);
        }
      }

      // Enforce max 2 tracks per artist across all phases, then take best 5
      const artistCounts = {};
      tracks = tracks.filter(t => {
        const key = (t.artist || '').toLowerCase();
        artistCounts[key] = (artistCounts[key] || 0) + 1;
        return artistCounts[key] <= 2;
      }).slice(0, 5);
    }

    // If Claude says there's no local scene (broad genre only), discard any hallucinated tracks
    if (spotlight.hasLocalScene === false && !spotlight.isNicheWorldGenre) tracks = [];
    const isNicheWorldGenre = spotlight.isNicheWorldGenre === true;
    let gsResult = { genre, country, explanation: spotlight.explanation, tracks, suggestedGenres: spotlight.suggestedGenres || [], hasLocalScene: spotlight.hasLocalScene !== false, isNicheWorldGenre };

    // Supplement with related artist tracks from the recommendation page (runs on fresh responses too)
    gsResult = await supplementFromRelatedArtists(gsResult);

    await storeCache(gsCacheKey, "genre-spotlight", gsResult);
    console.log(`[genre-spotlight] Claude → ${genre} / ${country} (${gsResult.tracks.length} tracks)`);
    res.json(gsResult);
  } catch (err) {
    console.error("Genre spotlight error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Take Me Deeper: suggest a niche subgenre ─────────────
app.post("/api/genre-deeper", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { genre, country, service = "spotify", visited = [] } = req.body;
  if (!genre || !country) return res.status(400).json({ error: "Missing genre or country." });

  const cacheKey = makeCacheKey(["genre-deeper", genre, country, ...(visited.length ? [visited.slice().sort().join(",")] : [])]);
  const cached = await getCached(cacheKey);
  if (cached) {
    console.log(`[genre-deeper] cache hit → ${genre} / ${country}`);
    return res.json(cached.result);
  }

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
        max_tokens: 200,
        system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks.",
        messages: [{
          role: "user",
          content: `A listener just explored "${genre}" from ${country} and wants to discover something new.
${visited.length > 0 ? `\nGenres already visited in this session (DO NOT suggest any of these or anything that is essentially the same genre under a different name): ${visited.map(g => `"${g}"`).join(", ")}\n` : ""}

Suggest ONE genre for them to explore next. Good moves include:
- A more specific subgenre (e.g. Rock → Shoegaze, Soul → Southern Soul)
- A closely related genre that shares roots, era, or audience (e.g. Reggae → Dub, Highlife → Jùjú)
- A regional or diaspora variant with a strong scene in a different country

Rules:
- MOST IMPORTANT: The suggestion must be MEANINGFULLY DIFFERENT from "${genre}" — not the same genre under a different name or label. Example failures: suggesting "Copperbelt Rock" when the input is "Zamrock", suggesting "Afro-Pop" when the input is "Afrobeats". The listener should feel like they are stepping somewhere genuinely new.
- Must be a real genre with a genuine music scene and real recordings — not an invented mashup of words
- Prefer genres that have actual tracks available on Spotify, Apple Music, or YouTube
- If "${genre}" is already extremely niche with no meaningful subgenres, suggest a related sibling or parent genre instead — do NOT suggest a synonym or rename
- Be specific enough to find real tracks (e.g. "Desert Blues" not "African Blues", "Lovers Rock" not "Reggae Pop")

Return exactly:
{
  "genre": "specific genre name",
  "country": "best country for this genre"
}`,
        }],
      }),
    });

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    let result;
    try {
      result = JSON.parse((data.content[0].text || "").replace(/```json|```/g, "").trim());
    } catch {
      return res.status(500).json({ error: "Failed to parse Claude response" });
    }

    result.genre = await resolveGenreCanonical(result.genre);
    await storeCache(cacheKey, "genre-deeper", result);
    console.log(`[genre-deeper] Claude → "${result.genre}" / ${result.country} (from ${genre} / ${country})`);
    res.json(result);
  } catch (err) {
    console.error("[genre-deeper] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Genre Artists — global artists for a genre ────────────
app.post("/api/genre-artists", async (req, res) => {
  const { genre } = req.body;
  if (!genre) return res.status(400).json({ error: "genre is required" });

  const cacheKey = makeCacheKey(["genre-artists", genre]);

  // Try cache first
  if (supabase) {
    const { data } = await supabase
      .from("recommendation_cache")
      .select("result, expires_at")
      .eq("cache_key", cacheKey)
      .maybeSingle();
    if (data?.result) {
      const expired = data.expires_at && new Date(data.expires_at) < new Date();
      if (!expired) {
        console.log(`[genre-artists] cache hit → ${genre}`);
        const artists = pickDiverseByEra(annotateTrackStatus(data.result.artists || []), 8);
        return res.json({ genre, artists });
      }
    }
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "API key not configured" });

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
        max_tokens: 2000,
        system:
          "You are a world music curator with deep knowledge of musical genres across all countries and eras. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [
          {
            role: "user",
            content: `List notable artists worldwide for the genre "${genre}".

Return exactly this JSON:
{
  "artists": [
    {
      "name": "Artist Name",
      "genre": "${genre}",
      "era": "1980s",
      "country": "Country name",
      "countryCode": "XX"
    }
  ]
}

Rules:
- Include exactly 12 artists from diverse countries — not just English-speaking or Western artists.
- era must be a decade string — exactly one of: 1900s, 1910s, 1920s, 1930s, 1940s, 1950s, 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s — the decade this artist was most active or is most associated with. Include artists from at least 3 different decade groups.
- Artists should be real, historically significant or culturally important for this genre in their country.
- Prioritize artists from countries where this genre originated or has a strong tradition.
- countryCode must be the correct ISO 3166-1 alpha-2 two-letter country code (e.g. "NG" for Nigeria, "BR" for Brazil).`,
          },
        ],
      }),
    });

    if (response.status === 529) {
      return res.status(503).json({ error: "Our servers are busy right now. Try again in a moment." });
    }

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const raw = (data.content[0].text || "").replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(raw);
    const artistsBase = parsed.artists || [];

    console.log(`[genre-artists] Claude → ${genre} (${artistsBase.length} artists)`);

    // Fetch artist image URLs (Spotify → Last.fm fallback)
    const imageUrls = await Promise.all(artistsBase.map(a => fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null)));
    const artists = artistsBase.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || undefined }));

    // Store in cache (30 day TTL)
    if (supabase) {
      const expires_at = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
      await supabase.from("recommendation_cache").upsert(
        { cache_key: cacheKey, endpoint: "genre-artists", result: { artists }, expires_at },
        { onConflict: "cache_key" }
      );
    }

    const annotated = annotateTrackStatus(artists);
    return res.json({ genre, artists: pickDiverseByEra(annotated, 8) });
  } catch (err) {
    console.error("[genre-artists] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Decade Spotlight: diverse global artists for a given decade ──────────────
app.post("/api/decade-spotlight", async (req, res) => {
  const { decade, service = "spotify" } = req.body;
  if (!decade) return res.status(400).json({ error: "decade is required" });

  const cacheKey = makeCacheKey(["decade-spotlight", decade]);

  if (supabase) {
    const { data } = await supabase
      .from("recommendation_cache")
      .select("result, expires_at")
      .eq("cache_key", cacheKey)
      .maybeSingle();
    if (data?.result) {
      const expired = data.expires_at && new Date(data.expires_at) < new Date();
      if (!expired) {
        console.log(`[decade-spotlight] cache hit → ${decade}`);
        return res.json({ decade, artists: annotateTrackStatus(data.result.artists || []) });
      }
    }
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "API key not configured" });

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
        system: "You are a world music curator with encyclopedic knowledge of global music history. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{
          role: "user",
          content: `You are curating a "Decade Spotlight" for the ${decade}. Select 16 artists who DEFINE THE SOUND of that decade — artists whose most iconic, career-defining recordings belong to the ${decade} and no other.

Return exactly this JSON:
{
  "artists": [
    {
      "name": "Artist Name",
      "genre": "their primary genre",
      "era": "${decade}",
      "country": "Country name",
      "countryCode": "XX",
      "similarTo": "A well-known Western artist the listener might know"
    }
  ]
}

THE CORE TEST — ask this for every artist before including them:
"If someone asks what decade THIS artist is most identified with, is the answer unambiguously the ${decade}?"
If the answer is any other decade, do NOT include them. Pick someone else.

EXAMPLES OF CORRECT THINKING:
- Fela Kuti → 1970s (Afrobeat peak). Include him in the 1970s list, NOT the 1960s or 1980s.
- Umm Kulthum → 1950s–1960s (died 1975). Include her in the 1960s list only, NOT the 1970s or 1980s.
- Bob Marley → 1970s. Do not put him in the 1960s or 1980s.
- Youssou N'Dour → 1980s–1990s. Include him in the 1980s or 1990s list, not earlier.

ERA RULES:
- The artist's most celebrated albums/recordings must have been released in ${decade.replace('s','')}–${parseInt(decade) + 9}.
- Artists whose peak fame is in an adjacent decade should go in that other decade's list.
- An artist who spans multiple decades should be assigned to the ONE decade they are most closely identified with.
- Do NOT include anyone who died before ${decade.replace('s','')}.

OTHER RULES:
- Exactly 16 artists. Cover ALL of these regions with at least 1–2 artists each: Sub-Saharan Africa, North Africa / Middle East, Latin America, Europe, South/Southeast Asia, East Asia, North America. Oceania is a bonus.
- Prioritise artists whose music is realistically available on streaming today.
- Include a wide variety of genres: pop, rock, folk, traditional, electronic, hip-hop, afrobeat, cumbia, etc.
- countryCode must be a valid ISO 3166-1 alpha-2 code.
- similarTo should help a Western listener understand the vibe — keep it brief (one artist name).
- No duplicate countries unless the country is very large (e.g. USA, Brazil, India can appear twice).`,
        }],
      }),
    });

    if (response.status === 529) {
      return res.status(503).json({ error: "Our servers are busy right now. Try again in a moment." });
    }

    const claudeData = await response.json();
    if (claudeData.error) return res.status(500).json({ error: claudeData.error.message });

    const raw = (claudeData.content[0].text || "").replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(raw);
    const artistsBase = parsed.artists || [];

    console.log(`[decade-spotlight] Claude → ${decade} (${artistsBase.length} artists)`);

    // Fetch artist images in parallel
    const imageUrls = await Promise.all(
      artistsBase.map(a => fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null))
    );
    const artists = artistsBase.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || undefined }));

    if (supabase) {
      const expires_at = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
      await supabase.from("recommendation_cache").upsert(
        { cache_key: cacheKey, endpoint: "decade-spotlight", result: { artists }, expires_at },
        { onConflict: "cache_key" }
      );
    }

    return res.json({ decade, artists: annotateTrackStatus(artists) });
  } catch (err) {
    console.error("[decade-spotlight] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Expert tester: flag a track for review ────────────────
app.post("/api/flag-track", async (req, res) => {
  const { trackTitle, trackArtist, spotifyId, appleId, country, genre, comment, userId } = req.body;
  if (!trackTitle || !country) return res.status(400).json({ error: "Missing required fields." });

  if (supabase) {
    await supabase.from("track_flags").insert({
      track_title: trackTitle,
      track_artist: trackArtist ?? null,
      spotify_id: spotifyId ?? null,
      apple_id: appleId ?? null,
      country,
      genre: genre ?? null,
      comment: comment ?? null,
      flagged_by: userId ?? null,
      created_at: new Date().toISOString(),
    });
  }

  console.log(`[flag-track] "${trackTitle}" by ${trackArtist} (${country}${genre ? ` / ${genre}` : ''}) flagged by ${userId ?? 'anonymous'}${comment ? `: "${comment}"` : ''}`);
  res.json({ ok: true });
});

// ── Cache bust endpoint ───────────────────────────────────
app.delete("/api/cache", async (req, res) => {
  const secret = req.headers["x-admin-secret"];
  if (secret !== process.env.ENRICH_SECRET) return res.status(401).json({ error: "Unauthorized" });
  if (!supabase) return res.status(500).json({ error: "No DB" });

  const { endpoint, country, genre } = req.query;

  // Build a LIKE pattern from the provided filters
  let query = supabase.from("recommendation_cache").delete();
  if (endpoint) query = query.eq("endpoint", endpoint);
  if (country && genre) {
    const key = makeCacheKey(["genrespotlight", genre, country]);
    query = query.like("cache_key", `${key}%`);
  } else if (country) {
    query = query.like("cache_key", `%${makeCacheKey([country])}%`);
  }

  const { error, count } = await query.select("*", { count: "exact", head: true });
  // Re-run as actual delete
  let del = supabase.from("recommendation_cache").delete();
  if (endpoint) del = del.eq("endpoint", endpoint);
  if (country && genre) {
    const key = makeCacheKey(["genrespotlight", genre, country]);
    del = del.like("cache_key", `${key}%`);
  } else if (country) {
    del = del.like("cache_key", `%${makeCacheKey([country])}%`);
  }
  const { error: delError } = await del;
  if (delError) return res.status(500).json({ error: delError.message });
  res.json({ ok: true, message: "Cache entries deleted" });
});

// ── Helper function to fetch artist tracks ──────────────
async function fetchArtistTracks(artistName) {
  return _fetchArtistTracksImpl(artistName);
}

async function _fetchArtistTracksImpl(artistName) {
  const lfCorrected = await lastfmArtistCorrection(artistName);
  if (lfCorrected) {
    console.log(`  [artist-tracks] Last.fm correction: "${artistName}" → "${lfCorrected}"`);
    artistName = lfCorrected;
  }

  const cacheKey = makeCacheKey(["artist-tracks", artistName]);

  // 1. In-memory cache
  const mem = artistTracksMemCache.get(cacheKey);
  if (mem && Date.now() - mem.cachedAt < ARTIST_TRACKS_TTL_MS && mem.tracks.length > 0) {
    console.log(`  [artist-tracks] mem cache hit → ${artistName}`);
    return mem.tracks;
  }

  // 2. Supabase cache
  const cached = await getCached(cacheKey);
  if (cached?.result?.tracks) {
    if (cached.result.tracks.length === 0 && cached.result.flagged) {
      console.log(`  [artist-tracks] cached empty (flagged) → re-queuing deep enrich for "${artistName}"`);
      reQueueForDeepEnrich(artistName).catch(() => {});
      return cached.result.tracks;
    } else {
      console.log(`  [artist-tracks] db cache hit → ${artistName}`);
      artistTracksMemCache.set(cacheKey, { tracks: cached.result.tracks, cachedAt: Date.now() });
      return cached.result.tracks;
    }
  }

  // 3. Apple Music → Deezer enrich
  console.log(`  [artist-tracks] fetching via Apple Music for "${artistName}"`);
  const appleToken = generateAppleMusicToken();
  const lfTitles = await lastfmArtistTopTracks(artistName, 5);
  let tracks = await proactiveArtistTracks(artistName, lfTitles, appleToken);

  if (tracks.length > 0) {
    tracks = await enrichWithDeezer(tracks, artistName);
    artistTracksMemCache.set(cacheKey, { tracks, cachedAt: Date.now() });
    storeCache(cacheKey, "artist-tracks", { tracks }).catch(() => {});
    console.log(`  [artist-tracks] Apple Music → ${tracks.length} tracks for "${artistName}"`);
    return tracks;
  }

  // 4. ListenBrainz → Deezer/YouTube enrichment as final fallback
  const lbTracks = await fetchArtistTracksFromLB(artistName);
  const enriched = await enrichTracksWithYouTube(lbTracks, artistName);
  if (enriched.length > 0) {
    artistTracksMemCache.set(cacheKey, { tracks: enriched, cachedAt: Date.now() });
    storeCache(cacheKey, "artist-tracks", { tracks: enriched }).catch(() => {});
  } else {
    storeCache(cacheKey, "artist-tracks", { tracks: [], flagged: true }).catch(() => {});
    console.log(`  [artist-tracks] no tracks found — flagged for deep enrich: "${artistName}"`);
  }
  return enriched;
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
  const cacheKey = makeCacheKey(["artist-tracks-apple", artistName]);

  // Memory cache
  const mem = artistTracksMemCache.get(cacheKey);
  if (mem && Date.now() - mem.cachedAt < ARTIST_TRACKS_TTL_MS) {
    console.log(`  [artist-tracks-apple] mem cache hit → ${artistName}`);
    return res.json({ tracks: mem.tracks });
  }
  // Supabase cache
  const cached = await getCached(cacheKey);
  if (cached?.result?.tracks) {
    if (cached.result.tracks.length === 0 && cached.result.flagged) {
      console.log(`  [artist-tracks-apple] cached empty (flagged) → re-queuing deep enrich for "${artistName}"`);
      reQueueForDeepEnrich(artistName).catch(() => {});
    } else {
      console.log(`  [artist-tracks-apple] db cache hit → ${artistName}`);
    }
    artistTracksMemCache.set(cacheKey, { tracks: cached.result.tracks, cachedAt: Date.now() });
    return res.json({ tracks: cached.result.tracks });
  }

  const token = generateAppleMusicToken();
  if (!token) return res.status(503).json({ error: "Apple Music not configured." });

  try {
    // Step 1: search for the artist by name (strip ensemble suffixes for better search results)
    const searchName = primaryArtistName(artistName);
    const artistSearch = await appleEnqueue(() => fetch(
      `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(searchName)}&types=artists&limit=5`,
      { headers: { Authorization: `Bearer ${token}` } }
    ));
    if (!artistSearch.ok) return res.json({ tracks: [] });
    const artistData = await artistSearch.json();
    const artists = artistData.results?.artists?.data || [];

    // Find the best-matching artist (name must closely match)
    const normalise = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const target = normalise(searchName);
    const matchedArtist = artists.find(a => normalise(a.attributes?.name || "") === target)
      ?? artists.find(a => {
        const n = normalise(a.attributes?.name || "");
        return n.includes(target) || (n.length >= 6 && target.includes(n)) || fuzzyArtistMatch(n, target);
      });

    let tracks = [];
    if (matchedArtist) {
      // Step 2: fetch top songs for the matched artist
      const artistId = matchedArtist.id;
      const songsRes = await appleEnqueue(() => fetch(
        `https://api.music.apple.com/v1/catalog/us/artists/${artistId}/view/top-songs?limit=5`,
        { headers: { Authorization: `Bearer ${token}` } }
      ));
      if (songsRes.ok) {
        const songsData = await songsRes.json();
        const songs = (songsData.data || []).slice(0, 3);
        tracks = songs.map(s => ({
          title:      s.attributes.name,
          artist:     s.attributes.artistName,
          appleId:    s.id,
          previewUrl: s.attributes.previews?.[0]?.url || null,
          embedUrl:   s.attributes.url.replace("music.apple.com", "embed.music.apple.com"),
        }));
      }
    }

    artistTracksMemCache.set(cacheKey, { tracks, cachedAt: Date.now() });
    if (tracks.length > 0) {
      storeCache(cacheKey, "artist-tracks-apple", { tracks }).catch(() => {});
      console.log(`  [artist-tracks-apple] cached ${tracks.length} tracks → ${artistName}`);
    } else {
      storeCache(cacheKey, "artist-tracks-apple", { tracks: [], flagged: true }).catch(() => {});
      console.log(`  [artist-tracks-apple] no tracks found — flagged for deep enrich: "${artistName}"`);
    }
    res.json({ tracks });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get tracks for a specific artist
app.get("/api/artist-tracks/:artistName", async (req, res) => {
  const { artistName } = req.params;
  if (!artistName) return res.status(400).json({ error: "Missing artist name" });

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

    if (!topArtistsResponse.ok) {
      return res.status(topArtistsResponse.status).json({ error: "Failed to fetch top artists" });
    }
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

  const lastfmKey = process.env.LASTFM_API_KEY;
  if (!lastfmKey) return res.status(503).json({ error: "Last.fm unavailable." });

  try {
    const r = await fetch(
      `https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(q)}&autocorrect=1&api_key=${lastfmKey}&format=json`
    );
    const data = await r.json();

    if (data.error || !data.artist?.name) {
      return res.status(404).json({ error: "Artist not found." });
    }

    const artist = data.artist;
    const name = artist.name;
    const genres = (artist.tags?.tag || []).map(t => t.name).filter(t => t.length < 40).slice(0, 3);
    const listeners = parseInt(artist.stats?.listeners || "0", 10);
    const imageUrl = await fetchArtistImageUrl(name, { genre: genres[0] || null, skipSpotify: true }).catch(() => null);

    res.json({
      id: name, // id unused by client — name is sufficient
      name,
      genres,
      imageUrl,
      followers: listeners,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Similar artists from around the world ────────────────

// Store reverse-index entries so pool members can find each other's pools
async function storeSimilarIndex(poolMembers, poolKey) {
  if (!supabase || !poolMembers?.length) return;
  const ttlMs = CACHE_TTL["similar-artists"];
  const expires_at = ttlMs ? new Date(Date.now() + ttlMs).toISOString() : null;
  const rows = poolMembers.map(a => ({
    cache_key: makeCacheKey(["similar-of", a.name]),
    endpoint: "similar-of",
    result: { poolKey },
    artist_pool: null,
    expires_at,
  }));
  await supabase.from("recommendation_cache").upsert(rows, { onConflict: "cache_key" });
}

app.post("/api/similar-artists", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { artistName } = req.body;
  if (!artistName) return res.status(400).json({ error: "Missing artistName." });

  const simCacheKey = makeCacheKey(["similar", artistName]);

  // Helper: fetch and attach missing images to a pool, update cache
  async function ensurePoolImages(pool, cacheKey, cacheResult) {
    const needsImages = pool.some(a => !a.imageUrl);
    if (!needsImages) return pool;
    const imageUrls = await Promise.all(
      pool.map(a => fetchArtistImageUrl(a.name, { genre: a.genre, skipSpotify: true }).catch(() => null))
    );
    const updated = pool.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || a.imageUrl || null }));
    await storeCache(cacheKey, "similar-artists", cacheResult, updated).catch(() => {});
    return updated;
  }

  // 1. Direct cache hit
  const simCached = await getCached(simCacheKey);
  if (simCached && simCached.artist_pool && simCached.artist_pool.length >= 4) {
    console.log(`[similar-artists] cache hit → ${artistName}`);
    const pool = await ensurePoolImages(simCached.artist_pool, simCacheKey, simCached.result);
    return res.json({
      baseArtist: simCached.result.baseArtist,
      artists: pickDiverse(pool, 5),
    });
  }

  // 2. Reverse-index hit — this artist appeared in someone else's pool
  const reverseEntry = await getCached(makeCacheKey(["similar-of", artistName]));
  if (reverseEntry?.result?.poolKey) {
    const sourcePool = await getCached(reverseEntry.result.poolKey);
    if (sourcePool?.artist_pool?.length >= 4) {
      console.log(`[similar-artists] reverse-index hit → ${artistName} from pool ${reverseEntry.result.poolKey}`);
      const filtered = sourcePool.artist_pool.filter(
        a => makeCacheKey([a.name]) !== makeCacheKey([sourcePool.result.baseArtist])
      );
      const pool = await ensurePoolImages(filtered, simCacheKey, { baseArtist: artistName });
      await storeCache(simCacheKey, "similar-artists", { baseArtist: artistName }, pool);
      return res.json({ baseArtist: artistName, artists: pickDiverse(pool, 5) });
    }
  }

  try {
    const lastfmKey = process.env.LASTFM_API_KEY;
    let foundName = artistName;
    let genres = [];
    let lastfmSimilar = [];
    let deezerSimilar = [];

    // Run Last.fm (info + similar) and Deezer (related) in parallel
    await Promise.all([
      (async () => {
        if (!lastfmKey) return;
        try {
          const [infoRes, simRes] = await Promise.all([
            fetch(`https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(artistName)}&autocorrect=1&api_key=${lastfmKey}&format=json`),
            fetch(`https://ws.audioscrobbler.com/2.0/?method=artist.getSimilar&artist=${encodeURIComponent(artistName)}&limit=30&autocorrect=1&api_key=${lastfmKey}&format=json`),
          ]);
          const [infoData, simData] = await Promise.all([infoRes.json(), simRes.json()]);

          if (!infoData.error && infoData.artist?.name) {
            foundName = infoData.artist.name;
            genres = (infoData.artist.tags?.tag || []).map(t => t.name).filter(t => t.length < 40).slice(0, 6);
            console.log(`[similar-artists] Last.fm info → ${foundName}, tags: ${genres.join(", ")}`);
          } else {
            console.log(`[similar-artists] Last.fm couldn't resolve "${artistName}" — using as-is`);
          }

          lastfmSimilar = (simData.similarartists?.artist || [])
            .map(a => ({ name: a.name, match: parseFloat(a.match) }))
            .filter(a => a.match > 0.1)
            .slice(0, 25);
          console.log(`[similar-artists] Last.fm similar → ${lastfmSimilar.length} artists for ${foundName}`);
        } catch (e) {
          console.log(`[similar-artists] Last.fm unavailable: ${e.message}`);
        }
      })(),
      (async () => {
        try {
          deezerSimilar = await deezerRelatedArtists(artistName);
          if (deezerSimilar.length > 0) console.log(`[similar-artists] Deezer related → ${deezerSimilar.length} artists`);
        } catch (e) {
          console.log(`[similar-artists] Deezer unavailable: ${e.message}`);
        }
      })(),
    ]);

    // ── Hybrid: use Last.fm artists with cached country data first, Claude fills gaps ──

    // Step 1: batch-lookup countries for Last.fm similar artists from cache + DB only
    // (getMbArtistCached never makes an API call — just memory + Supabase)
    const lfWithCountries = (await Promise.all(
      lastfmSimilar.map(async (a) => {
        const code = await getMbArtistCached(a.name);
        if (!code) return null;
        const region = REGION_BY_CODE[code];
        if (!region) return null;
        return { name: a.name, countryCode: code, country: ISO_TO_COUNTRY[code] || code, genre: genres[0] || '', era: null, match: a.match, region };
      })
    )).filter(Boolean);

    // Step 2: apply diversity rules — max 2 per region, 1 per country, best match first
    const regionCount = {};
    const usedCodes = new Set();
    const directPool = [];
    for (const a of [...lfWithCountries].sort((x, y) => y.match - x.match)) {
      if (directPool.length >= 10) break;
      if (usedCodes.has(a.countryCode)) continue;
      const used = regionCount[a.region] || 0;
      if (used >= 2) continue;
      regionCount[a.region] = used + 1;
      usedCodes.add(a.countryCode);
      directPool.push(a);
    }
    console.log(`[similar-artists] Last.fm cache hit: ${directPool.length} diverse artists with known countries`);

    // Step 3: identify missing continental requirements
    const hasAmericas = (regionCount['North America'] || 0) + (regionCount['Latin America'] || 0) > 0;
    const hasEurope   = (regionCount['Europe'] || 0) > 0;
    const hasAfricaOrME = (regionCount['Africa'] || 0) + (regionCount['Middle East'] || 0) > 0;
    const missingRegions = [
      ...(!hasAmericas    ? ['the Americas (North or Latin America)'] : []),
      ...(!hasEurope      ? ['Europe'] : []),
      ...(!hasAfricaOrME  ? ['Africa or the Middle East'] : []),
    ];

    let rawPool;

    if (directPool.length >= 8 && missingRegions.length === 0) {
      // All coverage requirements met — skip Claude entirely
      console.log(`[similar-artists] skipping Claude — full coverage from Last.fm country cache`);
      rawPool = directPool;
    } else {
      // Claude fills only the gaps
      const needCount = Math.max(10 - directPool.length, missingRegions.length * 2, 3);
      const alreadyCoveredLines = directPool.length > 0
        ? `\n\nArtists already sourced from Last.fm — DO NOT repeat these or their countries:\n${directPool.map(a => `- ${a.name} (${a.country})`).join('\n')}`
        : '';
      const gapInstruction = missingRegions.length > 0
        ? `You MUST include artists from these missing regions: ${missingRegions.join(', ')}.`
        : `Provide ${needCount} more diverse artists from underrepresented regions.`;

      const lastfmLines = lastfmSimilar.length > 0
        ? `\nLast.fm similar artists (sonic reference, ranked by similarity):\n${lastfmSimilar.map(a => `- ${a.name} (${(a.match * 100).toFixed(0)}%)`).join('\n')}`
        : '';
      const deezerLines = deezerSimilar.length > 0
        ? `\nDeezer related artists:\n${deezerSimilar.map(n => `- ${n}`).join('\n')}`
        : '';

      const prompt = `Find ${needCount} artists from different countries who sound similar to ${foundName}.

Their profile:
- Primary genres: ${genres.length > 0 ? genres.join(', ') : 'inferred from artist name'}
${lastfmLines}${deezerLines}${alreadyCoveredLines}

Rules:
- Each artist must be from a DIFFERENT country (and different from any already listed above)
- HARD LIMIT: No more than 2 artists from the same continent (Asia, Europe, Americas, Africa, Oceania)
- ${gapInstruction}
- Mix of contemporary and classic artists
- Avoid globally mainstream acts (no top-10 global chart artists)
- Even if the input artist is from a specific regional genre (K-pop, J-pop, etc.), find global equivalents who capture the same energy
- Be precise about country of origin — do NOT guess

Return ONLY valid JSON:
{
  "artists": [
    {
      "name": "exact artist name",
      "country": "full country name",
      "countryCode": "2-letter ISO code",
      "genre": "their primary genre",
      "era": "1980s (decade string: 1900s–2020s)"
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
      const claudeArtists = (JSON.parse(raw).artists || []);

      // Validate Claude-generated artists via Last.fm (hallucination filter)
      const validatedClaude = lastfmKey
        ? (await Promise.all(claudeArtists.map(async (artist) => {
            try {
              const r = await fetch(
                `https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(artist.name)}&autocorrect=0&api_key=${lastfmKey}&format=json`
              );
              const d = await r.json();
              if (d.error || !d.artist?.name) {
                console.log(`[similar-artists] filtered hallucination: ${artist.name}`);
                return null;
              }
              return artist;
            } catch { return artist; }
          }))).filter(Boolean)
        : claudeArtists;

      console.log(`[similar-artists] Claude validation: ${claudeArtists.length} → ${validatedClaude.length} artists`);

      // Merge: directPool (Last.fm, already verified) + Claude gap-fillers, deduplicating by country
      rawPool = [...directPool];
      for (const a of validatedClaude) {
        const duplicate = rawPool.some(
          x => x.countryCode === a.countryCode || x.name.toLowerCase() === a.name.toLowerCase()
        );
        if (!duplicate) rawPool.push(a);
      }
    }

    // Fetch images for the full merged pool — skip Spotify to avoid rate limit bleed from backfill script
    const imageUrls = await Promise.all(rawPool.map(a => fetchArtistImageUrl(a.name, { genre: a.genre, skipSpotify: true }).catch(() => null)));
    const poolWithImages = rawPool.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || null }));

    // Store pool immediately (with images, without MB country verification)
    await storeCache(simCacheKey, "similar-artists", { baseArtist: foundName }, poolWithImages);
    await storeSimilarIndex(poolWithImages, simCacheKey);
    console.log(`[similar-artists] → ${foundName} (${poolWithImages.length} in pool)`);
    res.json({ baseArtist: foundName, artists: pickDiverse(poolWithImages, 5) });

    // Country verification runs in background — corrects & re-caches without blocking the response
    verifyPoolCountries(poolWithImages).then(async verified => {
      const changed = verified.some((a, i) => a.countryCode !== poolWithImages[i]?.countryCode);
      if (changed) {
        await storeCache(simCacheKey, "similar-artists", { baseArtist: foundName }, verified);
        console.log(`[similar-artists] background country verify updated cache for ${foundName}`);
      }
    }).catch(() => {});
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
    console.log(`[insights] enriched Spotify data → ${enriched.totalArtists} artists`);
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
    console.log(`[insights] name-only fallback → ${topArtists.length} artists`);
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
    console.log(`[insights] Claude → ${result.dna?.length ?? 0} regions, ${result.blindSpots?.length ?? 0} blind spots, ${result.picks?.length ?? 0} picks`);
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
    supabase.from("user_stamps").select("country, stamped_at, visit_count, genre, source").eq("spotify_id", spotifyId).order("stamped_at", { ascending: false }),
    supabase.from("user_insights").select("*").eq("spotify_id", spotifyId).single(),
  ]);

  const insights = insightsResult.data;
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  const insightsFresh = insights &&
    insights.top_artists_hash === topArtistsHash &&
    (Date.now() - new Date(insights.created_at).getTime()) < sevenDays;

  res.json({
    favorites: (favResult.data || []).map(formatFavorite),
    stamps: (stampsResult.data || []).map(s => ({
      country: s.country,
      stampedAt: s.stamped_at,
      visitCount: s.visit_count ?? 1,
      genre: s.genre ?? null,
      source: s.source ?? null,
    })),
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
  const { data } = await supabase.from("user_stamps").select("country, stamped_at, visit_count, genre, source").eq("spotify_id", spotifyId).order("stamped_at", { ascending: false });
  res.json((data || []).map(s => ({
    country: s.country,
    stampedAt: s.stamped_at,
    visitCount: s.visit_count ?? 1,
    genre: s.genre ?? null,
    source: s.source ?? null,
  })));
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
  const { country, source, genre } = req.body;

  // Check if stamp already exists to increment visit_count
  const { data: existing } = await supabase
    .from("user_stamps")
    .select("visit_count")
    .eq("spotify_id", spotifyId)
    .eq("country", country)
    .single();

  if (existing) {
    await supabase
      .from("user_stamps")
      .update({ visit_count: (existing.visit_count || 1) + 1, source: source || null, genre: genre || null })
      .eq("spotify_id", spotifyId)
      .eq("country", country);
  } else {
    await supabase
      .from("user_stamps")
      .insert({ spotify_id: spotifyId, country, source: source || null, genre: genre || null, visit_count: 1 });
  }

  res.json({ ok: true });
});

// ── Country of the Day ───────────────────────────────────
const DAILY_COUNTRIES = [
  'Brazil', 'Japan', 'Nigeria', 'Cuba', 'Ethiopia', 'Colombia', 'Jamaica',
  'Iran', 'Mali', 'South Korea', 'Portugal', 'Iceland', 'Greece', 'Algeria',
  'India', 'Senegal', 'Vietnam', 'Argentina', 'Ghana', 'Turkey', 'Lebanon',
  'Morocco', 'Peru', 'Georgia', 'Mongolia', 'Cambodia', 'Cape Verde',
  'Trinidad & Tobago', 'Armenia', 'Azerbaijan', 'Laos', 'Papua New Guinea',
];

async function getOrCreateCountryOfDay(dateStr) {
  if (!supabase) {
    // Fallback: deterministic pick from date
    const day = Math.floor(Date.now() / (1000 * 60 * 60 * 24));
    return DAILY_COUNTRIES[day % DAILY_COUNTRIES.length];
  }

  // Return existing entry for today
  const { data: existing } = await supabase
    .from("country_of_day")
    .select("country")
    .eq("date", dateStr)
    .single();
  if (existing) return existing.country;

  // Pick least-recently-used country with lowest hit count
  const { data: recent } = await supabase
    .from("country_of_day")
    .select("country, hit_count")
    .order("date", { ascending: false })
    .limit(60);

  const recentSet = new Set((recent || []).map(r => r.country));
  const hitMap = {};
  (recent || []).forEach(r => { hitMap[r.country] = (hitMap[r.country] || 0) + r.hit_count; });

  // Prefer countries not used recently
  let candidates = DAILY_COUNTRIES.filter(c => !recentSet.has(c));
  if (candidates.length === 0) candidates = [...DAILY_COUNTRIES];

  // Pick lowest hit count among candidates
  candidates.sort((a, b) => (hitMap[a] || 0) - (hitMap[b] || 0));
  const country = candidates[0];

  await supabase.from("country_of_day").insert({ date: dateStr, country, hit_count: 0 });
  return country;
}

// ── Country data enrichment ───────────────────────────────
// Finds countries with missing or weak music data and proactively
// researches and stores tracks so users never hit an empty screen.

const ALL_ENRICHABLE_COUNTRIES = [
  'Brazil','Japan','Nigeria','Cuba','Ethiopia','Colombia','Jamaica','Iran','Mali',
  'South Korea','Portugal','Iceland','Greece','Algeria','India','Senegal','Vietnam',
  'Argentina','Ghana','Turkey','Lebanon','Morocco','Peru','Georgia','Mongolia',
  'Cambodia','Cape Verde','Trinidad & Tobago','Armenia','Azerbaijan','Laos',
  'Papua New Guinea','Togo','Benin','Burkina Faso','Niger','Chad','Sudan','Eritrea',
  'Djibouti','Somalia','Mozambique','Zambia','Malawi','Rwanda','Burundi','Uganda',
  'Tanzania','Cameroon','Congo','DR Congo','Ivory Coast','Sierra Leone','Guinea',
  'Guinea-Bissau','Gambia','Mauritania','Liberia','Equatorial Guinea','Gabon',
  'Central African Republic','Angola','Namibia','Botswana','Zimbabwe','Lesotho',
  'Eswatini','Madagascar','Comoros','Seychelles','Mauritius','Réunion',
  'Kazakhstan','Uzbekistan','Turkmenistan','Tajikistan','Kyrgyzstan','Afghanistan',
  'Pakistan','Bangladesh','Nepal','Sri Lanka','Myanmar','Thailand','Philippines',
  'Indonesia','Malaysia','Brunei','East Timor','Papua New Guinea',
  'Fiji','Tonga','Samoa','Vanuatu','Solomon Islands','Kiribati','Palau',
  'Bolivia','Paraguay','Uruguay','Ecuador','Venezuela','Guyana','Suriname',
  'Honduras','El Salvador','Guatemala','Nicaragua','Costa Rica','Panama',
  'Dominican Republic','Haiti','Barbados','Trinidad & Tobago','Martinique','Guadeloupe',
  'Tunisia','Libya','Egypt','Jordan','Iraq','Syria','Yemen','Oman','Kuwait',
  'Qatar','Bahrain','UAE','Saudi Arabia','Palestine','Cyprus','Malta',
  'Albania','Bosnia','North Macedonia','Kosovo','Moldova','Belarus',
  'Latvia','Lithuania','Estonia','Slovenia','Croatia','Serbia','Montenegro',
  'Slovakia','Czech Republic','Hungary','Romania','Bulgaria','Ukraine',
  'New Zealand','Australia','Canada','Mexico',
];

async function findWeakCountries(limit = 2) {
  if (!supabase) return ALL_ENRICHABLE_COUNTRIES.slice(0, limit);

  // Get all existing recommend cache keys
  const { data: cached } = await supabase
    .from("recommendation_cache")
    .select("cache_key")
    .eq("endpoint", "recommend");

  const cachedKeys = new Set((cached || []).map(r => r.cache_key));

  // Countries with zero recommend data get highest priority
  const uncached = ALL_ENRICHABLE_COUNTRIES.filter(
    c => !cachedKeys.has(makeCacheKey(["recommend", c]))
  );

  if (uncached.length >= limit) {
    return uncached.sort(() => Math.random() - 0.5).slice(0, limit);
  }

  // Among cached countries, find those where many artists failed enrichment
  const { data: failed } = await supabase
    .from("enrichment_queue")
    .select("country")
    .gte("attempts", 3)
    .is("completed_at", null);

  const failCounts = {};
  for (const r of failed || []) {
    failCounts[r.country] = (failCounts[r.country] || 0) + 1;
  }

  const weakCached = ALL_ENRICHABLE_COUNTRIES
    .filter(c => cachedKeys.has(makeCacheKey(["recommend", c])) && (failCounts[c] || 0) >= 4)
    .sort((a, b) => (failCounts[b] || 0) - (failCounts[a] || 0));

  return [...uncached, ...weakCached].slice(0, limit);
}

// Fetch tracks for one artist using Apple Music (2-step: artist search → top songs)
// Falls back to ListenBrainz if Apple Music doesn't carry the artist.
// Search Apple Music across storefronts for a song query, returning the first match.
// Strip ensemble/feat suffixes so "Mulatu Astatqé & His Ethiopian Quintet" → "Mulatu Astatqé"
// and "Dexter Story feat. Hamelmal Abate" → "Dexter Story".
// Only strips "& His/Her/The/Their..." (ensemble markers) — preserves "Simon & Garfunkel".
function primaryArtistName(name) {
  return name
    .replace(/\s+(?:feat\.|ft\.|featuring)\s+.*/i, '')
    .replace(/\s+&\s+(?:his|her|the|their)\b.*/i, '')
    .trim() || name;
}

// Apply any name normalization to every artist object in a list.
function normalizeArtistNames(artists) {
  return artists.map(a => ({ ...a, name: a.name.trim() }));
}

// Allow 1-character difference for names of similar length — handles transliteration
// variants like "Beqele" vs "Bekele" (Amharic q/k) without risking false positives.
function fuzzyArtistMatch(a, b) {
  if (a === b) return true;
  if (Math.abs(a.length - b.length) > 1 || a.length < 7) return false;
  let diffs = 0;
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    if (a[i] !== b[i] && ++diffs > 1) return false;
  }
  return diffs <= 1;
}

// Storefronts tried in order — covers US, Europe, Middle East, South Asia, East Asia, Latin America.
const APPLE_STOREFRONTS = ['us', 'gb', 'sa', 'ae', 'bh', 'eg', 'in', 'jp', 'kr', 'br', 'mx', 'de'];

async function appleSongSearch(query, artistTarget, appleToken) {
  const normalise = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const target = normalise(primaryArtistName(artistTarget));
  for (const sf of APPLE_STOREFRONTS) {
    try {
      const r = await appleEnqueue(() => fetch(
        `https://api.music.apple.com/v1/catalog/${sf}/search?term=${encodeURIComponent(query)}&types=songs&limit=3`,
        { headers: { Authorization: `Bearer ${appleToken}` } }
      ));
      if (!r.ok) continue;
      const d = await r.json();
      const songs = d.results?.songs?.data || [];
      // Prefer songs where the target is the PRIMARY artist (name starts with or equals theirs),
      // only fall back to featured appearances if nothing else matches.
      const primaryMatch = songs.find(s => {
        const n = normalise(s.attributes.artistName);
        return n === target || n.startsWith(target) || fuzzyArtistMatch(n, target);
      });
      const featuredMatch = primaryMatch ? null : songs.find(s => {
        const n = normalise(s.attributes.artistName);
        return n.includes(target) || (n.length >= 6 && target.includes(n));
      });
      const match = primaryMatch || featuredMatch;
      if (match) return match;
    } catch { /* try next storefront */ }
  }
  return null;
}

async function proactiveArtistTracks(artistName, knownTracks = [], appleToken) {
  const normalise = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
  // Strip ensemble suffixes for search/matching: "Mulatu Astatqé & His Ethiopian Quintet" → "Mulatu Astatqé"
  const searchName = primaryArtistName(artistName);
  const target = normalise(searchName);

  // 1. Apple Music: search for the artist across all storefronts
  if (appleToken) {
    try {
      for (const sf of APPLE_STOREFRONTS) {
        const r = await appleEnqueue(() => fetch(
          `https://api.music.apple.com/v1/catalog/${sf}/search?term=${encodeURIComponent(searchName)}&types=artists&limit=5`,
          { headers: { Authorization: `Bearer ${appleToken}` } }
        ));
        if (!r.ok) continue;
        const d = await r.json();
        const artists = d.results?.artists?.data || [];
        const matched = artists.find(a => normalise(a.attributes?.name || "") === target)
          ?? artists.find(a => {
            const n = normalise(a.attributes?.name || "");
            return n.includes(target) || (n.length >= 6 && target.includes(n)) || fuzzyArtistMatch(n, target);
          });

        if (matched) {
          const songsRes = await appleEnqueue(() => fetch(
            `https://api.music.apple.com/v1/catalog/${sf}/artists/${matched.id}/view/top-songs?limit=5`,
            { headers: { Authorization: `Bearer ${appleToken}` } }
          ));
          if (songsRes.ok) {
            const sd = await songsRes.json();
            const songs = sd.data || [];
            if (songs.length > 0) {
              // Prefer songs where this artist is the PRIMARY artist (artistName starts with
              // or equals their name), not tracks they merely appear on as a featured guest.
              const ownSongs = songs.filter(s => {
                const an = normalise(s.attributes?.artistName || "");
                return an === target || an.startsWith(target);
              });
              const toReturn = ownSongs.length >= 2 ? ownSongs : songs;
              return toReturn.slice(0, 3).map(s => ({
                title: s.attributes.name,
                artist: s.attributes.artistName,
                appleId: s.id,
                previewUrl: s.attributes.previews?.[0]?.url || null,
                embedUrl: s.attributes.url.replace("music.apple.com", "embed.music.apple.com"),
              }));
            }
          }
        }
      }

      // Artist not found on Apple Music — try searching for their known tracks directly
      if (knownTracks.length > 0) {
        const results = [];
        for (const trackTitle of knownTracks.slice(0, 2)) {
          const match = await appleSongSearch(`${trackTitle} ${artistName}`, artistName, appleToken);
          if (match) results.push({
            title: match.attributes.name,
            artist: match.attributes.artistName,
            appleId: match.id,
            previewUrl: match.attributes.previews?.[0]?.url || null,
            embedUrl: match.attributes.url.replace("music.apple.com", "embed.music.apple.com"),
          });
        }
        if (results.length > 0) return results;
      }
    } catch { /* fall through to Deezer */ }
  }

  // 2. Deezer — free, no auth, 30s MP3 previews work on all platforms including Android
  const deezerTracks = await deezerArtistTopTracks(artistName);
  if (deezerTracks.length > 0) return deezerTracks;

  // 3. ListenBrainz fallback — returns title+artist without streaming IDs
  const lbTracks = await fetchArtistTracksFromLB(artistName);
  if (lbTracks.length > 0 && appleToken) {
    // Enrich LB tracks with Apple Music IDs via per-track song search (us then gb)
    const enriched = await Promise.all(lbTracks.slice(0, 3).map(async (track) => {
      try {
        const match = await appleSongSearch(`${track.title} ${artistName}`, artistName, appleToken);
        if (match) {
          return {
            ...track,
            appleId: match.id,
            previewUrl: match.attributes.previews?.[0]?.url || track.previewUrl || null,
            embedUrl: match.attributes.url.replace("music.apple.com", "embed.music.apple.com"),
          };
        }
      } catch { /* keep as-is */ }
      return track;
    }));
    return enriched;
  }
  // Enrich LB-only tracks (no streaming IDs) with YouTube video URLs
  if (lbTracks.length > 0) return enrichTracksWithYouTube(lbTracks, artistName);
  return [];
}

// Fetch tracks for one artist using Spotify (2-step: artist search → top tracks).
// Does NOT restrict to market=US so non-Western artists are found globally.
// Falls back to ListenBrainz if Spotify doesn't carry the artist.
// Routes through spotifyEnqueue so parallel callers don't flood the API.
function proactiveSpotifyTracks(artistName) {
  return spotifyEnqueue(() => _proactiveSpotifyTracksImpl(artistName));
}
async function _proactiveSpotifyTracksImpl(artistName) {
  const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  const target = normalize(artistName);

  try {
    const accessToken = await getClientAccessToken();
    if (!accessToken) return [];

    // Step 1: search for the artist without market restriction
    const artistSearch = await spotifyFetch(
      `https://api.spotify.com/v1/search?q=${encodeURIComponent(artistName)}&type=artist&limit=5`,
      { headers: { Authorization: "Bearer " + accessToken } }
    );
    if (!artistSearch.ok) return fetchArtistTracksFromLB(artistName);

    const artistData = await artistSearch.json();
    const artists = artistData.artists?.items || [];

    // Require name match to prevent false positives (e.g. "Oraz Tagan" → Polish children's music)
    const matched = artists.find(a => normalize(a.name) === target)
      ?? artists.find(a => {
        const n = normalize(a.name);
        return n.includes(target) || target.includes(n);
      });

    if (!matched) {
      console.log(`  [proactive-spotify] no artist match for "${artistName}" → LB fallback`);
      return fetchArtistTracksFromLB(artistName);
    }

    console.log(`  [proactive-spotify] matched "${matched.name}" (${matched.id})`);

    // Step 2: fetch top tracks — market=US required with client credentials (no user market)
    const topTracksRes = await spotifyFetch(
      `https://api.spotify.com/v1/artists/${matched.id}/top-tracks?market=US`,
      { headers: { Authorization: "Bearer " + accessToken } }
    );

    let proTracks = [];
    if (topTracksRes.ok) {
      const topData = await topTracksRes.json();
      proTracks = (topData.tracks || []).slice(0, 3).map(t => ({
        title: t.name,
        artist: t.artists?.[0]?.name,
        spotifyId: t.id,
        previewUrl: t.preview_url || null,
        spotifyUrl: `https://open.spotify.com/track/${t.id}`,
      }));
    }

    // top-tracks?market=US misses non-US licensed artists — try track search fallback
    if (proTracks.length === 0) {
      console.log(`  [proactive-spotify] top-tracks empty for "${artistName}", trying search fallback`);
      const searchRes = await spotifyFetch(
        `https://api.spotify.com/v1/search?q=${encodeURIComponent(`artist:${artistName}`)}&type=track&limit=5`,
        { headers: { Authorization: "Bearer " + accessToken } }
      );
      if (searchRes.ok) {
        const sd = await searchRes.json();
        const items = (sd.tracks?.items || []).filter(t =>
          t.artists?.some(a => normalize(a.name) === target || normalize(a.name).includes(target) || target.includes(normalize(a.name)))
        );
        proTracks = items.slice(0, 3).map(t => ({
          title: t.name,
          artist: t.artists?.[0]?.name,
          spotifyId: t.id,
          previewUrl: t.preview_url || null,
          spotifyUrl: `https://open.spotify.com/track/${t.id}`,
        }));
        if (proTracks.length > 0) console.log(`  [proactive-spotify] search fallback found ${proTracks.length} tracks for "${artistName}"`);
      }
    }

    if (proTracks.length > 0) return proTracks;

    // Last resort: ListenBrainz → enrich with YouTube
    console.log(`  [proactive-spotify] no tracks from Spotify for "${artistName}" → LB fallback`);
    const lbTracks = await fetchArtistTracksFromLB(artistName);
    return enrichTracksWithYouTube(lbTracks, artistName);
  } catch (err) {
    console.error(`  [proactive-spotify] error for "${artistName}":`, err.message);
    const lbTracks = await fetchArtistTracksFromLB(artistName);
    return enrichTracksWithYouTube(lbTracks, artistName);
  }
}

// Reset an artist's enrichment_queue entry so the next cron run retries it with deep search.
async function reQueueForDeepEnrich(artistName) {
  if (!supabase) return;
  await supabase.from("enrichment_queue")
    .update({ attempts: 0, completed_at: null, last_attempted_at: null })
    .eq("artist", artistName)
    .is("completed_at", null);
}

// ── Last.fm helpers ───────────────────────────────────────

// Returns up to `limit` top track titles for an artist from Last.fm.
// Excellent coverage for non-Western artists thanks to global scrobbling.
async function lastfmArtistTopTracks(artistName, limit = 5) {
  const key = process.env.LASTFM_API_KEY;
  if (!key) return [];
  try {
    const r = await fetch(
      `https://ws.audioscrobbler.com/2.0/?method=artist.getTopTracks&artist=${encodeURIComponent(artistName)}&limit=${limit}&autocorrect=1&api_key=${key}&format=json`
    );
    const d = await r.json();
    if (d.error) return [];
    return (d.toptracks?.track || []).map(t => t.name);
  } catch { return []; }
}

// Returns top artist names for a country from Last.fm geo.getTopArtists.
// Uses real scrobble data — much more reliable than Claude alone for country attribution.
async function lastfmGeoTopArtists(country, limit = 25) {
  const key = process.env.LASTFM_API_KEY;
  if (!key) return [];
  try {
    const r = await fetch(
      `https://ws.audioscrobbler.com/2.0/?method=geo.getTopArtists&country=${encodeURIComponent(country)}&limit=${limit}&api_key=${key}&format=json`
    );
    const d = await r.json();
    if (d.error) return [];
    return (d.topartists?.artist || []).map(a => a.name);
  } catch { return []; }
}

// Returns the canonical artist name from Last.fm (e.g. "the beatles" → "The Beatles").
// Returns null if no correction found or Last.fm key is missing.
async function lastfmArtistCorrection(artistName) {
  const key = process.env.LASTFM_API_KEY;
  if (!key) return null;
  try {
    const r = await fetch(
      `https://ws.audioscrobbler.com/2.0/?method=artist.getCorrection&artist=${encodeURIComponent(artistName)}&api_key=${key}&format=json`
    );
    const d = await r.json();
    if (d.error) return null;
    const corrected = d.corrections?.correction?.artist?.name;
    if (corrected && corrected.toLowerCase() !== artistName.toLowerCase()) {
      return corrected;
    }
    return null;
  } catch { return null; }
}

// ── Deezer helpers ─────────────────────────────────────────
// No API key required. Rate limit: 50 req/5s per IP.
// Preview URLs are plain 30s MP3s — work on iOS, Android, and web.
const DEEZER_BASE = 'https://api.deezer.com';
const deezerQueue = [];
let deezerBusy = false;

function deezerEnqueue(fn) {
  return new Promise((resolve, reject) => {
    deezerQueue.push({ fn, resolve, reject });
    if (!deezerBusy) drainDeezerQueue();
  });
}

async function drainDeezerQueue() {
  if (deezerQueue.length === 0) { deezerBusy = false; return; }
  deezerBusy = true;
  const { fn, resolve, reject } = deezerQueue.shift();
  try { resolve(await fn()); } catch (e) { reject(e); }
  setTimeout(drainDeezerQueue, 150); // 6.7/s → well within 50/5s limit
}

// ── Apple Music API queue ─────────────────────────────────
// No published hard rate limit, but concurrent calls cause 429s under load.
// 100ms gap → 10 req/s; safe for background enrichment while serving live requests.
const appleQueue = [];
let appleBusy = false;

function appleEnqueue(fn) {
  return new Promise((resolve, reject) => {
    appleQueue.push({ fn, resolve, reject });
    if (!appleBusy) drainAppleQueue();
  });
}

async function drainAppleQueue() {
  if (appleQueue.length === 0) { appleBusy = false; return; }
  appleBusy = true;
  const { fn, resolve, reject } = appleQueue.shift();
  try { resolve(await fn()); } catch (e) { reject(e); }
  setTimeout(drainAppleQueue, 100);
}

// Returns top tracks for an artist from Deezer with 30s preview MP3 URLs.
async function deezerArtistTopTracks(artistName, limit = 5) {
  try {
    const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
    const target = normalise(artistName);

    // Step 1: find the artist
    const searchRes = await deezerEnqueue(() =>
      fetch(`${DEEZER_BASE}/search/artist?q=${encodeURIComponent(artistName)}&limit=5`)
    );
    const searchData = await searchRes.json();
    if (searchData.error?.code === 4) { console.warn('  [Deezer] quota exceeded'); return []; }

    const artists = searchData.data || [];
    const matched = artists.find(a => normalise(a.name) === target)
      ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || (n.length >= 5 && target.includes(n)); });

    if (!matched) { console.log(`  [Deezer] no artist match for "${artistName}"`); return []; }

    // Step 2: get top tracks
    const topRes = await deezerEnqueue(() =>
      fetch(`${DEEZER_BASE}/artist/${matched.id}/top?limit=${limit}`)
    );
    const topData = await topRes.json();
    if (topData.error?.code === 4) return [];

    const allWithPreview = (topData.data || []).filter(t => t.preview);
    // Prefer tracks where this artist is the main artist, not just a featured guest
    const ownTracks = allWithPreview.filter(t => t.artist?.id === matched.id);
    const tracks = (ownTracks.length >= 2 ? ownTracks : allWithPreview)
      .slice(0, 3)
      .map(t => ({
        title: t.title_short || t.title,
        artist: t.artist?.name || artistName,
        previewUrl: t.preview,
        deezerId: String(t.id),
        deezerUrl: t.link,
      }));

    if (tracks.length > 0) console.log(`  [Deezer] ${tracks.length} tracks for "${artistName}"`);
    return tracks;
  } catch (err) {
    console.error(`  [Deezer] error for "${artistName}":`, err.message);
    return [];
  }
}

// Enriches any tracks missing a deezerId by calling deezerTrackSearch per track.
async function enrichWithDeezer(tracks, artistName) {
  return Promise.all(tracks.map(async (t) => {
    if (t.deezerId || t.deezerUrl) return t;
    const found = await deezerTrackSearch(t.title, artistName);
    return found ? { ...t, ...found } : t;
  }));
}

// Search Deezer for a specific track by title + artist. Returns a track object with previewUrl or null.
async function deezerTrackSearch(trackTitle, artistName) {
  try {
    const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
    const target = normalise(artistName);
    const r = await deezerEnqueue(() =>
      fetch(`${DEEZER_BASE}/search?q=artist:"${encodeURIComponent(artistName)}" track:"${encodeURIComponent(trackTitle)}"&limit=5`)
    );
    const d = await r.json();
    if (d.error?.code === 4) return null;
    const items = (d.data || []).filter(t =>
      t.preview && (normalise(t.artist?.name || '').includes(target) || target.includes(normalise(t.artist?.name || '')))
    );
    const best = items[0];
    if (!best) return null;
    return {
      title: best.title_short || best.title,
      artist: best.artist?.name || artistName,
      previewUrl: best.preview,
      deezerId: String(best.id),
      deezerUrl: best.link,
    };
  } catch { return null; }
}

async function deezerRelatedArtists(artistName) {
  try {
    const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
    const target = normalise(artistName);

    const searchRes = await deezerEnqueue(() =>
      fetch(`${DEEZER_BASE}/search/artist?q=${encodeURIComponent(artistName)}&limit=5`)
    );
    const searchData = await searchRes.json();
    if (searchData.error?.code === 4) { console.warn('  [Deezer related] quota exceeded'); return []; }

    const artists = searchData.data || [];
    const matched = artists.find(a => normalise(a.name) === target)
      ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || (n.length >= 5 && target.includes(n)); });
    if (!matched) return [];

    const relRes = await deezerEnqueue(() =>
      fetch(`${DEEZER_BASE}/artist/${matched.id}/related?limit=20`)
    );
    const relData = await relRes.json();
    if (relData.error?.code === 4) return [];

    return (relData.data || []).map(a => a.name).filter(Boolean).slice(0, 20);
  } catch (err) {
    console.error(`  [Deezer related] error for "${artistName}":`, err.message);
    return [];
  }
}

// ── YouTube helpers ────────────────────────────────────────

// Search YouTube Data API v3 for the best music video matching a query.
// Returns a videoId string or null. Each call costs 100 quota units (10k/day free).
async function youtubeVideoSearch(query) {
  const key = process.env.YOUTUBE_API_KEY;
  if (!key) return null;
  try {
    const r = await fetch(
      `https://www.googleapis.com/youtube/v3/search?q=${encodeURIComponent(query)}&type=video&videoCategoryId=10&part=snippet&maxResults=1&key=${key}`
    );
    if (!r.ok) return null;
    const d = await r.json();
    return d.items?.[0]?.id?.videoId || null;
  } catch { return null; }
}

// Enriches tracks that have no streaming IDs (LB-only results).
// Tries Deezer first (free, cross-platform MP3 preview) then YouTube as final fallback.
async function enrichTracksWithYouTube(tracks, artistName) {
  return Promise.all(tracks.map(async (track) => {
    if (track.previewUrl || track.appleId || track.spotifyId || track.deezerId) return track;
    // 1. Deezer — actual audio preview, no quota cost, works on Android
    const deezerResult = await deezerTrackSearch(track.title, artistName);
    if (deezerResult) {
      console.log(`  [deezer] preview found for "${track.title}" by "${artistName}"`);
      return { ...track, ...deezerResult };
    }
    // 2. YouTube — link only, last resort
    if (!process.env.YOUTUBE_API_KEY) return track;
    const videoId = await youtubeVideoSearch(`${track.title} ${artistName} official`);
    if (videoId) {
      console.log(`  [youtube] found video for "${track.title}" by "${artistName}": ${videoId}`);
      return { ...track, youtubeUrl: `https://www.youtube.com/watch?v=${videoId}` };
    }
    return track;
  }));
}

// Find all artist-tracks cache entries flagged as empty, then try much harder to find tracks.
// Uses Last.fm to surface specific song titles first, falls back to Claude then LB.
// Fetch and patch an artist image into every recommend pool entry that contains
// this artist but has no imageUrl.  Uses genre from the pool entry to help
// disambiguate same-name artists (e.g. "Prince" the musician vs. anything else).
async function patchArtistImageIfMissing(artistName) {
  if (!supabase) return;
  try {
    // Find recommend entries whose artist_pool text contains the artist name.
    // The ilike cast avoids a full table scan and is good enough for exact names.
    const { data: rows } = await supabase
      .from("recommendation_cache")
      .select("id, artist_pool")
      .eq("endpoint", "recommend")
      .filter("artist_pool::text", "ilike", `%"${artistName}"%`);

    if (!rows?.length) return;

    for (const row of rows) {
      if (!Array.isArray(row.artist_pool)) continue;
      const normName = s => (s || "").toLowerCase();
      const idx = row.artist_pool.findIndex(a => normName(a.name) === normName(artistName));
      if (idx === -1) continue;
      if (row.artist_pool[idx].imageUrl) continue; // already has one

      const genre = row.artist_pool[idx].genre || null;
      const imageUrl = await fetchArtistImageUrl(artistName, { genre }).catch(() => null);
      if (!imageUrl) continue;

      const newPool = row.artist_pool.map((a, i) =>
        i === idx ? { ...a, imageUrl } : a
      );
      await supabase
        .from("recommendation_cache")
        .update({ artist_pool: newPool })
        .eq("id", row.id);
      console.log(`[deep-enrich] image patched for "${artistName}" (genre: ${genre || "unknown"})`);
    }
  } catch (e) {
    console.warn(`[deep-enrich] image patch failed for "${artistName}":`, e.message);
  }
}

async function deepEnrichFlaggedArtists(apiKey, limit = 5) {
  if (!supabase) return { processed: 0 };

  // Find flagged empty entries across both track endpoints
  const { data: flagged } = await supabase
    .from("recommendation_cache")
    .select("cache_key, endpoint")
    .in("endpoint", ["artist-tracks", "artist-tracks-apple"])
    .eq("result->>flagged", "true")
    .limit(limit * 2); // fetch extra — some may fail

  if (!flagged?.length) return { processed: 0 };

  // Deduplicate by artist name (same artist may be flagged in both endpoints)
  const seen = new Set();
  const toEnrich = [];
  for (const row of flagged) {
    const prefix = row.endpoint === "artist-tracks-apple" ? "artist-tracks-apple_" : "artist-tracks_";
    const slug = row.cache_key.replace(prefix, "");
    if (!seen.has(slug)) { seen.add(slug); toEnrich.push({ slug, endpoint: row.endpoint, cacheKey: row.cache_key }); }
    if (toEnrich.length >= limit) break;
  }

  console.log(`[deep-enrich] ${toEnrich.length} flagged artists to retry`);
  const appleToken = generateAppleMusicToken();
  let processed = 0;

  for (const { slug, endpoint, cacheKey } of toEnrich) {
    // Reconstruct artist name from slug (hyphens back to spaces, best effort)
    const rawName = slug.replace(/-/g, " ");
    // Resolve canonical name via Last.fm before any lookups
    const corrected = await lastfmArtistCorrection(rawName);
    const artistName = corrected || rawName;
    if (corrected) console.log(`[deep-enrich] Last.fm correction: "${rawName}" → "${corrected}"`);

    // Step 1a: Last.fm artist.getTopTracks — fast, free, great non-Western coverage
    let knownTitles = await lastfmArtistTopTracks(artistName, 10);
    if (knownTitles.length > 0) {
      console.log(`[deep-enrich] Last.fm → ${knownTitles.length} known tracks for "${artistName}"`);
    } else {
      // Step 1b: Fall back to Claude Haiku only if Last.fm has nothing
      try {
        const cr = await fetch("https://api.anthropic.com/v1/messages", {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
          body: JSON.stringify({
            model: "claude-haiku-4-5-20251001",
            max_tokens: 150,
            system: "Return ONLY valid JSON — no markdown, no preamble.",
            messages: [{ role: "user", content: `What are 3 real, specific song titles that the artist "${artistName}" is best known for? If you don't know this artist, return an empty list. JSON: {"titles": ["Title 1", "Title 2", "Title 3"]}` }],
          }),
        });
        const cd = await cr.json();
        const raw = (cd.content?.[0]?.text || "").replace(/```json|```/g, "").trim();
        knownTitles = JSON.parse(raw).titles || [];
        if (knownTitles.length > 0) console.log(`[deep-enrich] Claude → ${knownTitles.length} known tracks for "${artistName}"`);
      } catch { /* proceed without titles */ }
    }

    // Step 2: search for the artist + known titles across Apple Music and Spotify
    const isApple = endpoint === "artist-tracks-apple";
    let tracks = await proactiveArtistTracks(artistName, knownTitles, appleToken);

    // Step 3: if Apple Music returned nothing, try Spotify directly
    if (tracks.length === 0) {
      tracks = await (async () => {
        const token = await getClientAccessToken();
        if (!token) return [];
        // Try each known title with artist name on Spotify
        const results = [];
        for (const title of knownTitles.slice(0, 5)) {
          const r = await spotifyFetch(
            `https://api.spotify.com/v1/search?q=${encodeURIComponent(`track:"${title}" artist:"${artistName}"`)}&type=track&limit=3`,
            { headers: { Authorization: "Bearer " + token } }
          );
          if (!r.ok) continue;
          const d = await r.json();
          const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, "");
          const match = (d.tracks?.items || []).find(t =>
            t.artists.some(a => norm(a.name).includes(norm(artistName)) || norm(artistName).includes(norm(a.name)))
          );
          if (match) results.push({ title: match.name, spotifyId: match.id, previewUrl: match.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${match.id}` });
        }
        return results;
      })();
    }

    // Step 4: full Spotify artist-search path (top-tracks → track-search → LB → YouTube)
    // proactiveArtistTracks already tried Deezer + LB; this covers the Spotify angle
    // and enriches any LB results with YouTube preview URLs as a last resort
    if (tracks.length === 0) tracks = await proactiveSpotifyTracks(artistName);

    if (tracks.length > 0) {
      // Clear flag — we found tracks
      const storeEndpoint = isApple ? "artist-tracks-apple" : "artist-tracks";
      artistTracksMemCache.set(cacheKey, { tracks, cachedAt: Date.now() });
      await storeCache(cacheKey, storeEndpoint, { tracks });
      // Mark as complete in enrichment queue
      if (supabase) {
        await supabase.from("enrichment_queue")
          .update({ completed_at: new Date().toISOString() })
          .eq("artist", artistName);
      }
      console.log(`[deep-enrich] ✓ "${artistName}" → ${tracks.length} tracks`);
      backfillDeezerForArtists([artistName]).catch(() => {});
      patchArtistImageIfMissing(artistName).catch(() => {});
      processed++;
    } else {
      console.log(`[deep-enrich] – "${artistName}" still no tracks — keeping flagged`);
    }

    await new Promise(r => setTimeout(r, 500));
  }

  return { processed, total: toEnrich.length };
}

async function deepEnrichCountry(country, apiKey) {
  console.log(`[country-enrich] Researching ${country}...`);
  const appleToken = generateAppleMusicToken();

  // Step 1a: Pull Last.fm geo.getTopArtists to ground Claude in real scrobble data
  const lfArtists = await lastfmGeoTopArtists(country, 25);
  const lfNote = lfArtists.length > 0
    ? `\n\nLast.fm top artists for ${country} (verified by global scrobbling data — these artists are genuinely associated with ${country}):\n${lfArtists.join(', ')}\n\nPrioritize these artists in your response. You may add others not on this list only if you are certain they are from ${country}.`
    : '';
  if (lfArtists.length > 0) console.log(`[country-enrich] Last.fm → ${lfArtists.length} artists for ${country}`);

  // Step 1b: Claude researches the country's music scene in depth
  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({
      model: "claude-opus-4-5-20251101",
      max_tokens: 3500,
      system: "You are a world music ethnomusicologist with encyclopedic knowledge of music from every country. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
      messages: [{
        role: "user",
        content: `Deep music research for "${country}".

Every artist MUST be genuinely from ${country} — born there or the group formed there. No exceptions.

Return exactly this JSON:
{
  "genres": ["main genre", "secondary genre", "tertiary genre"],
  "didYouKnow": "one genuinely surprising fact about ${country}'s music history",
  "artists": [
    {
      "name": "Artist Name",
      "genre": "specific local genre",
      "era": "1980s",
      "similarTo": "one well-known comparison artist name only",
      "knownTracks": ["Exact Song Title 1", "Exact Song Title 2"],
      "likelyOnStreaming": true
    }
  ]
}

era must be a decade string — exactly one of: 1900s, 1910s, 1920s, 1930s, 1940s, 1950s, 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s — the decade this artist was most active or is most associated with.
Include 12 artists with a varied spread of decades — include artists from at least 3 different decade groups.
knownTracks: real specific song titles this artist is known for (used to find them on Spotify/Apple Music).
likelyOnStreaming: true if you believe this artist has a presence on Spotify or Apple Music; false for purely regional or very obscure artists.
IMPORTANT: Use the artist's exact real name as it appears on streaming platforms. Do NOT repeat a name (e.g. write "Banah" not "Banah Banah") unless the repeated form is the actual official name (e.g. "Duran Duran", "Talk Talk" are correct).

Also include:
"streamingFloor": the earliest decade where ${country} has notable music genuinely available on Spotify/Apple Music. Must be exactly one of: 1900s, 1910s, 1920s, 1930s, 1940s, 1950s, 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s. Consider whether pre-war or early-era recordings from ${country} are actually digitized and on streaming. Major hubs (USA, UK, France, Brazil, Cuba, Argentina, Nigeria, Jamaica): as early as 1920s–1950s. Most countries: 1970s–1990s.${lfNote}`
      }]
    }),
  });

  const claudeData = await claudeRes.json();
  if (claudeData.error) throw new Error(`Claude error: ${claudeData.error.message}`);
  const raw = (claudeData.content[0].text || "").replace(/```json|```/g, "").trim();
  let research;
  try { research = JSON.parse(raw); } catch (parseErr) {
    console.error(`[country-enrich] JSON.parse failed for ${country}:`, parseErr.message, "\nRaw:", raw.slice(0, 200));
    throw parseErr;
  }
  research.artists = normalizeArtistNames(research.artists);

  console.log(`[country-enrich] ${country}: ${research.artists.length} artists from Claude`);

  // Step 1c: Auto-improve Claude's streamingFloor estimate by cross-checking knownTracks on Spotify
  let streamingFloor = DECADES_LIST.includes(research.streamingFloor) ? research.streamingFloor : '1980s';
  try {
    const clientToken = await getClientAccessToken();
    if (clientToken) {
      const artistsToCheck = research.artists.filter(a => a.knownTracks?.length).slice(0, 4);
      for (const artist of artistsToCheck) {
        try {
          const sq = `track:${artist.knownTracks[0]} artist:${artist.name}`;
          const r = await fetch(
            `https://api.spotify.com/v1/search?q=${encodeURIComponent(sq)}&type=track&limit=1`,
            { headers: { Authorization: 'Bearer ' + clientToken } }
          );
          const d = await r.json();
          const releaseDate = d.tracks?.items?.[0]?.album?.release_date;
          if (releaseDate) {
            const decade = yearToDecade(parseInt(releaseDate.slice(0, 4)));
            if (decade && decadeIndex(decade) < decadeIndex(streamingFloor)) {
              console.log(`[streaming-floor] ${country}: ${artist.name} → ${decade}, improving floor from ${streamingFloor}`);
              streamingFloor = decade;
            }
          }
          await new Promise(r => setTimeout(r, 150));
        } catch {}
      }
    }
  } catch {}
  console.log(`[streaming-floor] ${country}: final floor = ${streamingFloor}`);

  // Step 2: Fetch image URLs for all artists (preserve any already cached)
  const cacheKey = makeCacheKey(["recommend", country]);
  const existingCache = await getCached(cacheKey);
  const existingImageMap = {};
  for (const a of (existingCache?.artistPool || [])) {
    if (a.name && a.imageUrl) existingImageMap[a.name.toLowerCase()] = a.imageUrl;
  }

  const artistsWithImages = await Promise.all(research.artists.map(async (a) => {
    const existing = existingImageMap[a.name.toLowerCase()];
    if (existing) return { ...a, imageUrl: existing };
    const imageUrl = await fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null);
    return imageUrl ? { ...a, imageUrl } : a;
  }));
  console.log(`[country-enrich] ${country}: fetched images for ${artistsWithImages.filter(a => a.imageUrl).length}/${artistsWithImages.length} artists`);

  await storeCache(cacheKey, "recommend",
    { genres: research.genres, didYouKnow: research.didYouKnow, streamingFloor },
    artistsWithImages
  );

  // Step 3: Proactively fetch and cache tracks for each artist
  const successfulArtists = [];  // artists that have playable tracks
  const failedArtists = [];      // artists with no tracks found

  for (const artist of artistsWithImages) {
    const artistCacheKey = makeCacheKey(["artist-tracks-apple", artist.name]);

    // Skip if already cached
    const existing = await getCached(artistCacheKey);
    if (existing?.result?.tracks?.length > 0) {
      successfulArtists.push(artist);
      continue;
    }

    const tracks = await proactiveArtistTracks(artist.name, artist.knownTracks || [], appleToken);

    if (tracks.length > 0) {
      artistTracksMemCache.set(artistCacheKey, { tracks, cachedAt: Date.now() });
      await storeCache(artistCacheKey, "artist-tracks-apple", { tracks });
      console.log(`  [country-enrich] ✓ ${artist.name} → ${tracks.length} tracks${tracks[0]?.appleId ? " (Apple Music)" : " (LB)"}`);
      successfulArtists.push(artist);
    } else {
      console.log(`  [country-enrich] – ${artist.name} → no tracks found`);
      await addToEnrichmentQueue([artist], country);
      failedArtists.push(artist);
    }

    await new Promise(r => setTimeout(r, 300));
  }

  // Step 4: Replace unplayable artists — aim for at least 8 with tracks
  const TARGET_WITH_TRACKS = 8;
  const needed = Math.max(0, TARGET_WITH_TRACKS - successfulArtists.length);
  const replacements = [];

  if (needed > 0 && failedArtists.length > 0) {
    console.log(`[country-enrich] ${country}: ${failedArtists.length} failed — finding ${needed} replacements`);
    const allUsedNames = artistsWithImages.map(a => a.name);

    // Ask Claude Haiku for replacements grounded in Last.fm geo data
    const lfCandidates = lfArtists.filter(n => !allUsedNames.some(u => u.toLowerCase() === n.toLowerCase()));
    const lfHint = lfCandidates.length > 0
      ? `\nLast.fm top artists for ${country} not yet used: ${lfCandidates.slice(0, 15).join(', ')}. Prefer these if they are genuinely from ${country}.`
      : '';

    try {
      const cr = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
        body: JSON.stringify({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 500,
          system: "Return ONLY valid JSON — no markdown, no preamble.",
          messages: [{
            role: "user",
            content: `Suggest ${needed + 2} replacement artists from ${country} who ARE on Spotify or Apple Music.
Do NOT suggest: ${allUsedNames.join(', ')}.
Strongly prefer Contemporary artists (active in the last 20 years) who release on streaming platforms.${lfHint}
JSON: {"artists": [{"name": "Artist Name", "genre": "genre", "era": "Contemporary", "knownTracks": ["Track 1", "Track 2"]}]}`
          }]
        }),
      });
      const cd = await cr.json();
      const raw = (cd.content?.[0]?.text || "").replace(/```json|```/g, "").trim();
      const candidates = JSON.parse(raw).artists || [];
      console.log(`[country-enrich] replacement candidates for ${country}: ${candidates.map(a => a.name).join(', ')}`);

      for (const candidate of candidates) {
        if (replacements.length >= needed) break;
        const ck = makeCacheKey(["artist-tracks-apple", candidate.name]);
        const lfTitles = await lastfmArtistTopTracks(candidate.name, 5);
        const seedTracks = [...new Set([...(candidate.knownTracks || []), ...lfTitles])];
        const tracks = await proactiveArtistTracks(candidate.name, seedTracks, appleToken);
        if (tracks.length > 0) {
          artistTracksMemCache.set(ck, { tracks, cachedAt: Date.now() });
          await storeCache(ck, "artist-tracks-apple", { tracks });
          replacements.push(candidate);
          console.log(`  [country-enrich] ✓ replacement ${candidate.name} → ${tracks.length} tracks`);
        } else {
          console.log(`  [country-enrich] – replacement ${candidate.name} → still no tracks`);
        }
        await new Promise(r => setTimeout(r, 300));
      }
    } catch (err) {
      console.error(`[country-enrich] replacement search failed for ${country}:`, err.message);
    }
  }

  // Update cached pool: swap in replacements for failed artists, keep rest
  if (replacements.length > 0) {
    const updatedPool = [
      ...successfulArtists,
      ...replacements,
      ...failedArtists.slice(replacements.length), // keep any still-unresolved failures at end
    ];
    await storeCache(cacheKey, "recommend",
      { genres: research.genres, didYouKnow: research.didYouKnow, streamingFloor },
      updatedPool
    );
    console.log(`[country-enrich] ${country}: updated pool with ${replacements.length} replacements`);
  }

  const withTracks = successfulArtists.length + replacements.length;
  const withoutTracks = failedArtists.length - replacements.length;
  console.log(`[country-enrich] ${country} done: ${withTracks} with tracks, ${withoutTracks} without`);
  return { country, artists: research.artists.length, withTracks, withoutTracks };
}

// GET /api/streaming-floors — public, returns { country: decade } for all countries with a known floor
app.get("/api/streaming-floors", async (req, res) => {
  if (!supabase) return res.json({});
  // Build cache_key → country reverse map from COUNTRY_ISO
  const keyToCountry = {};
  const cacheKeys = [];
  for (const country of [...new Set(Object.keys(COUNTRY_ISO))]) {
    const key = makeCacheKey(["recommend", country]);
    keyToCountry[key] = country;
    cacheKeys.push(key);
  }
  const { data, error } = await supabase
    .from("recommendation_cache")
    .select("cache_key, result")
    .in("cache_key", cacheKeys);
  if (error) return res.status(500).json({ error: error.message });
  const floors = {};
  for (const row of (data || [])) {
    const country = keyToCountry[row.cache_key];
    if (country && row.result?.streamingFloor) floors[country] = row.result.streamingFloor;
  }
  res.json(floors);
});

// GET /api/populate-streaming-floors?secret=...  — background job: cheap Claude call per country missing a floor
app.get("/api/populate-streaming-floors", async (req, res) => {
  if (!req.query.secret || req.query.secret !== process.env.ENRICH_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY not set" });

  // Enumerate all known modern countries from COUNTRY_ISO
  const countries = [...new Set(Object.keys(COUNTRY_ISO))];
  const toProcess = [];
  for (const country of countries) {
    const cacheKey = makeCacheKey(["recommend", country]);
    const { data } = await supabase.from("recommendation_cache")
      .select("result").eq("cache_key", cacheKey).single();
    if (data?.result && !data.result.streamingFloor) toProcess.push({ country, result: data.result });
  }

  console.log(`[streaming-floor-job] ${toProcess.length} countries need floors (${countries.length - toProcess.length} already have them)`);
  res.json({ total: countries.length, toProcess: toProcess.length, message: "Job started in background — check server logs" });

  // Run in background with rate-limit-friendly delays
  setImmediate(async () => {
    let done = 0;
    for (const { country, result } of toProcess) {
      try {
        const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
          body: JSON.stringify({
            model: "claude-haiku-4-5-20251001",
            max_tokens: 10,
            messages: [{
              role: "user",
              content: `What is the earliest decade where ${country} has notable music genuinely available on Spotify/Apple Music? Reply with ONLY the decade like "1970s". Major hubs (USA, UK, France, Brazil, Cuba, Jamaica, Nigeria): as early as 1920s–1950s. Most countries: 1970s–1990s.`
            }]
          }),
        });
        const claudeData = await claudeRes.json();
        const raw = (claudeData.content?.[0]?.text || "").trim().replace(/[^0-9s]/g, '');
        const floor = DECADES_LIST.find(d => d === raw + 's') || DECADES_LIST.find(d => raw.startsWith(d.slice(0, 4)));
        if (floor) {
          await supabase.from("recommendation_cache")
            .update({ result: { ...result, streamingFloor: floor } })
            .eq("cache_key", makeCacheKey(["recommend", country]));
          console.log(`[streaming-floor-job] ✓ ${country} → ${floor} (${++done}/${toProcess.length})`);
        } else {
          console.warn(`[streaming-floor-job] ✗ ${country}: couldn't parse "${claudeData.content?.[0]?.text}"`);
        }
      } catch (err) {
        console.error(`[streaming-floor-job] error for ${country}:`, err.message);
      }
      await new Promise(r => setTimeout(r, 2500)); // 2.5s between calls — well within Anthropic rate limits
    }
    console.log(`[streaming-floor-job] done. ${done}/${toProcess.length} floors populated.`);
  });
});

// GET /api/backfill-deezer?secret=...&limit=20
// Enriches cached artist-tracks entries with Deezer IDs by title-matching.
app.get("/api/backfill-deezer", async (req, res) => {
  if (!req.query.secret || req.query.secret !== process.env.ENRICH_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  if (!supabase) return res.status(503).json({ error: "No DB" });

  const limit = Math.min(parseInt(req.query.limit || "20", 10), 100);

  const { data: rows, error } = await supabase
    .from("recommendation_cache")
    .select("result")
    .in("endpoint", ["artist-tracks", "artist-tracks-apple"])
    .not("result->tracks", "is", null)
    .limit(limit);

  if (error) return res.status(500).json({ error: error.message });

  const artistNames = [...new Set(
    rows
      .filter(r => Array.isArray(r.result?.tracks) && !r.result.tracks.every(t => t.deezerId))
      .map(r => r.result.tracks[0]?.artist)
      .filter(Boolean)
  )];

  res.json({ message: `Processing ${artistNames.length} artists in background` });
  backfillDeezerForArtists(artistNames).catch(() => {});
});

// GET /api/enrich-countries?secret=...&count=2
// GET /api/enrich-countries?secret=...&country=Azerbaijan  (target a specific country)
app.get("/api/enrich-countries", async (req, res) => {
  if (!req.query.secret || req.query.secret !== process.env.ENRICH_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY not set" });

  try {
    let targets;
    if (req.query.country) {
      targets = [req.query.country];
      console.log(`[country-enrich] Targeting specific country: ${req.query.country}`);
    } else {
      const count = Math.min(parseInt(req.query.count || "2", 10), 5);
      targets = await findWeakCountries(count);
      if (targets.length === 0) {
        return res.json({ message: "All countries have sufficient data", enriched: [] });
      }
      console.log(`[country-enrich] Weak countries to enrich: ${targets.join(", ")}`);
    }

    const results = [];
    for (const country of targets) {
      try {
        const result = await deepEnrichCountry(country, apiKey);
        results.push(result);
      } catch (err) {
        console.error(`[country-enrich] Error enriching ${country}:`, err.message);
        results.push({ country, error: err.message });
      }
    }

    res.json({ enriched: results });
  } catch (err) {
    console.error("[country-enrich] Fatal error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/country-of-day", async (req, res) => {
  const dateStr = req.query.date || new Date().toISOString().slice(0, 10);
  const country = await getOrCreateCountryOfDay(dateStr);
  res.json({ date: dateStr, country });
});

// Returns the next N days of country-of-day entries, creating any that don't exist yet.
// Used by the mobile app to pre-schedule accurate push notifications.
app.get("/api/country-of-day/upcoming", async (req, res) => {
  const days = Math.min(parseInt(req.query.days || "7", 10), 30);
  const results = [];
  for (let i = 0; i < days; i++) {
    const d = new Date();
    d.setDate(d.getDate() + i);
    const dateStr = d.toISOString().slice(0, 10);
    const country = await getOrCreateCountryOfDay(dateStr);
    results.push({ date: dateStr, country });
  }
  res.json(results);
});

app.post("/api/country-of-day/hit", async (req, res) => {
  if (!supabase) return res.json({ ok: true });
  const { date } = req.body;
  const dateStr = date || new Date().toISOString().slice(0, 10);
  const { error } = await supabase.rpc("increment_country_hit", { target_date: dateStr });
  if (error) {
    // Fallback if RPC not yet created: read-then-write increment
    const { data } = await supabase.from("country_of_day").select("hit_count").eq("date", dateStr).single();
    if (data) {
      await supabase.from("country_of_day").update({ hit_count: data.hit_count + 1 }).eq("date", dateStr);
    }
  }
  res.json({ ok: true });
});

// ── Background enrichment endpoint ───────────────────────
// Called by cron job (Railway scheduler or cron-job.org)
// GET /api/enrich?secret=YOUR_ENRICH_SECRET&batch=5
// Optionally skip flag review: &review_flags=false
app.get("/api/enrich", async (req, res) => {
  const secret = req.query.secret;
  if (!secret || secret !== process.env.ENRICH_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const batchSize = Math.min(parseInt(req.query.batch || "5", 10), 20);
  const skipFlagReview = req.query.review_flags === "false";

  try {
    // Priority 1: deep-enrich artists that returned 0 tracks (flagged in cache)
    const deepResult = await deepEnrichFlaggedArtists(process.env.ANTHROPIC_API_KEY, 5);
    if (deepResult.total > 0) {
      console.log(`[enrich] deep-enrich: ${deepResult.processed}/${deepResult.total} flagged artists resolved`);
    }

    // Priority 2: regular enrichment queue
    const enrichResult = await processEnrichmentBatch(batchSize);
    console.log(`[enrich] batch complete → ${enrichResult.processed}/${enrichResult.total} enriched`);

    // Purge expired cache rows
    const { count: purged } = await supabase
      .from("recommendation_cache")
      .delete()
      .lt("expires_at", new Date().toISOString());
    if (purged) console.log(`[enrich] purged ${purged} expired cache rows`);

    let flagResult = { reviewed: 0, fixed: 0, skipped: skipFlagReview };
    if (!skipFlagReview) {
      // Run flag review after enrichment. Max 2 contexts per cron run to
      // avoid timeout — each context makes 2 Claude calls + Spotify lookups.
      flagResult = await processFlaggedTracks(2);
    }

    res.json({ enrich: enrichResult, deepEnrich: deepResult, flagReview: flagResult, purged: purged ?? 0 });
  } catch (err) {
    console.error("[enrich] batch error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Manual flag review endpoint ───────────────────────────
// Trigger on-demand: GET /api/review-flags?secret=...&contexts=5
app.get("/api/review-flags", async (req, res) => {
  const secret = req.query.secret;
  if (!secret || secret !== process.env.ENRICH_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const maxContexts = Math.min(parseInt(req.query.contexts || "5", 10), 10);

  try {
    const result = await processFlaggedTracks(maxContexts);
    res.json(result);
  } catch (err) {
    console.error("[review-flags] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Backfill image URLs for cached artists that don't have one ──────────────
// Runs once at startup, well after the server is ready. Processes one artist
// at a time with a delay to stay within Spotify rate limits (~100 req/min).
async function backfillArtistImageUrls() {
  if (!supabase) return;
  console.log('[image-backfill] starting...');

  const { data: rows, error } = await supabase
    .from('recommendation_cache')
    .select('cache_key, endpoint, result, artist_pool')
    .in('endpoint', ['recommend', 'genre-artists']);

  if (error || !rows) { console.log('[image-backfill] could not fetch cache rows:', error?.message); return; }

  const delay = ms => new Promise(r => setTimeout(r, ms));

  for (const row of rows) {
    // Pick the artists array from whichever column holds it
    const isRecommend = row.endpoint === 'recommend';
    const artists = isRecommend ? (row.artist_pool || []) : (row.result?.artists || []);

    const missing = artists.filter(a => !a.imageUrl);
    if (missing.length === 0) continue;

    console.log(`[image-backfill] ${row.cache_key}: fetching ${missing.length} missing image(s)`);

    let changed = false;
    for (const artist of missing) {
      await delay(1500); // Apple Music + Last.fm only — no Spotify to avoid competing with track verification
      const url = await fetchArtistImageUrl(artist.name, { skipSpotify: true }).catch(() => null);
      if (url) { artist.imageUrl = url; changed = true; }
    }

    if (!changed) continue;

    // Write back the updated artists to the correct column
    const update = isRecommend
      ? { artist_pool: artists }
      : { result: { ...row.result, artists } };

    await supabase.from('recommendation_cache').update(update).eq('cache_key', row.cache_key);
    console.log(`[image-backfill] updated ${row.cache_key}`);
  }

  console.log('[image-backfill] done');
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎵 Musical Passport running at http://localhost:${PORT}\n`);
  // Image backfill disabled — re-enable by uncommenting when needed
  // setTimeout(() => backfillArtistImageUrls().catch(err => console.error('[image-backfill] error:', err.message)), 15_000);
});
