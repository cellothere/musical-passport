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

function makeCacheKey(parts) {
  return parts.filter(Boolean).join("_").toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9_-]/g, "");
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
        artists: pickRandom(cached.artist_pool, 4),
        didYouKnow: cached.result.didYouKnow,
      });
    }
  }

  // Get user's top artists and liked songs if authenticated
  let topArtists = [];
  let likedSongs = [];

  if (accessToken) {
    // Fetch top artists and a random sample of liked songs in parallel
    await Promise.all([
      // Top 10 artists
      fetch(
        "https://api.spotify.com/v1/me/top/artists?limit=10&time_range=medium_term",
        { headers: { Authorization: "Bearer " + accessToken } }
      ).then(async (r) => {
        if (r.ok) {
          const d = await r.json();
          topArtists = d.items ? d.items.map((a) => a.name) : [];
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
  const songsNote = likedSongs.length > 0
    ? `\nSome songs from their liked songs library: ${likedSongs.join("; ")}.`
    : "";
  const personalizationNote = (artistNote || songsNote)
    ? `\n\nPersonalization context for this user:${artistNote}${songsNote}\nUse this to personalize the "similarTo" comparisons to artists and styles they will recognize.`
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
        max_tokens: accessToken ? 1000 : 2500,
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
      "similarTo": "A well-known artist the listener might know"
    }
  ],
  "didYouKnow": "One surprising musical fact about ${country}"
}
era must be exactly one of: Contemporary, Golden Era, Pioneer. Include exactly ${accessToken ? 4 : 12} artists mixing eras.${personalizationNote}`,
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

    if (!accessToken) {
      const cacheKey = makeCacheKey(["recommend", country]);
      await storeCache(cacheKey, "recommend", { genres: rec.genres, didYouKnow: rec.didYouKnow }, rec.artists);
      return res.json({ genres: rec.genres, artists: pickRandom(rec.artists, 4), didYouKnow: rec.didYouKnow });
    }

    res.json(rec);
  } catch (err) {
    console.error("Error:", err);
    res.status(500).json({ error: err.message });
  }
});

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
  if (tmCached) return res.json(tmCached.result);

  try {
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
  "description": "2 vivid sentences explaining the connection between ${country} and the ${decade} for this genre — acknowledge any anachronism and explain what the creative angle is",
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
            if (s1) return { ...track, appleId: s1.id, previewUrl: s1.attributes.previews?.[0]?.url || null, embedUrl: s1.attributes.url.replace("music.apple.com", "embed.music.apple.com") };

            // Pass 2: title only
            const r2 = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(track.title)}&types=songs&limit=1`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d2 = await r2.json();
            const s2 = d2.results?.songs?.data?.[0];
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
            if (found1) return { ...track, spotifyId: found1.id, previewUrl: found1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found1.id}` };

            const q2 = `${track.title} ${track.artist}`;
            const r2 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(q2)}&type=track&limit=1&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d2 = await r2.json();
            const found2 = d2.tracks?.items?.[0];
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

    const tmResult = { genre: spotlight.genre, description: spotlight.description, tracks: validTracks };
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
  "explanation": "2-3 crisp sentences: what defines this genre in ${country}, its roots, and why it matters",
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
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q)}&types=songs&limit=1`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d = await r.json();
            const s = d.results?.songs?.data?.[0];
            return s
              ? { ...track, appleId: s.id, previewUrl: s.attributes.previews?.[0]?.url || null }
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
            if (found1) return { ...track, spotifyId: found1.id, previewUrl: found1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found1.id}` };

            const r2 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(`${track.title} ${track.artist}`)}&type=track&limit=1&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d2 = await r2.json();
            const found2 = d2.tracks?.items?.[0];
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
    const trackIds = (tracksData.tracks?.items || []).slice(0, 5).map(t => t.id);

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

    // Step 4: Build Claude prompt
    const profileLines = audioProfile
      ? `Spotify audio profile (averaged across top tracks):
- Danceability: ${audioProfile.danceability} / 1.0
- Energy: ${audioProfile.energy} / 1.0
- Valence (positivity): ${audioProfile.valence} / 1.0
- Acousticness: ${audioProfile.acousticness} / 1.0
- Tempo: ${audioProfile.tempo} BPM`
      : "";

    const prompt = `Find 12 artists from different countries around the world who sound similar to ${foundName}.

Their profile:
- Primary genres: ${genres.length > 0 ? genres.join(", ") : "mainstream pop"}
${profileLines}

Rules:
- Each artist must be from a DIFFERENT country
- Spread across at least 5 different continents
- Mix of contemporary and classic artists
- Avoid globally mainstream acts (no top-10 global chart artists)
- Focus on capturing the same sonic energy, emotional feel, or stylistic DNA

Return ONLY valid JSON:
{
  "baseArtist": "${foundName}",
  "sonicSummary": "2 sentences describing what defines their sound and why people love it",
  "artists": [
    {
      "name": "exact artist name as on Spotify",
      "country": "full country name",
      "countryCode": "2-letter ISO code",
      "genre": "their primary genre",
      "era": "Contemporary|Golden Era|Pioneer",
      "description": "2 sentences on their sound",
      "similarityReason": "1 specific sentence on why they match ${foundName}'s energy"
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
        max_tokens: 4500,
        system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const claudeData = await claudeRes.json();
    if (claudeData.error) return res.status(500).json({ error: claudeData.error.message });

    const raw = (claudeData.content[0].text || "").replace(/```json|```/g, "").trim();
    const result = JSON.parse(raw);

    const simMeta = { baseArtist: result.baseArtist || foundName, sonicSummary: result.sonicSummary || "" };
    const simPool = result.artists || [];
    await storeCache(simCacheKey, "similar-artists", simMeta, simPool);
    res.json({ ...simMeta, artists: pickDiverse(simPool, 4) });
  } catch (err) {
    console.error("Similar artists error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`\n🎵 Musical Passport running at http://localhost:${PORT}\n`);
});
