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

function extractBalancedJsonBlock(text) {
  if (!text) return "";
  const cleaned = String(text).replace(/```json|```/gi, "").trim();
  const start = cleaned.search(/[\[{]/);
  if (start === -1) return cleaned;

  const stack = [];
  let inString = false;
  let escaped = false;

  for (let i = start; i < cleaned.length; i++) {
    const ch = cleaned[i];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === "\"") {
        inString = false;
      }
      continue;
    }

    if (ch === "\"") {
      inString = true;
      continue;
    }

    if (ch === "{" || ch === "[") {
      stack.push(ch);
      continue;
    }

    if (ch === "}" || ch === "]") {
      const expected = ch === "}" ? "{" : "[";
      if (stack[stack.length - 1] !== expected) break;
      stack.pop();
      if (stack.length === 0) return cleaned.slice(start, i + 1);
    }
  }

  return cleaned;
}

function parseClaudeJson(text, label = "Claude response") {
  const candidate = extractBalancedJsonBlock(text);
  try {
    return JSON.parse(candidate);
  } catch (err) {
    const preview = String(text || "").replace(/\s+/g, " ").trim().slice(0, 240);
    err.message = `${err.message} [${label}] Raw: ${preview}`;
    throw err;
  }
}

// ── Artist image URL fetching (Apple Music → Last.fm → Deezer) ──
// Priority: Apple Music → Last.fm → Deezer.
// Spotify is intentionally NOT used as a source: Spotify Developer Terms §IV.4
// prohibit caching Spotify Content (which includes images) beyond what is
// necessary for direct end-user use, and our recommendation_cache persists
// imageUrl values long-term inside artist_pool JSON.
// The skipSpotify parameter is retained for backwards compatibility with
// callers but is now a no-op.
async function fetchArtistImageUrl(artistName, { skipSpotify: _skipSpotify = false, genre = null } = {}) {
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
      const r = await lfFetch(
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
    const d = await deezerEnqueue(() =>
      deezerFetchJson(`/search/artist?q=${encodeURIComponent(term)}&limit=5`, 'Deezer image')
    );
    if (!d || d.error?.code === 4) continue;
    const artists = d.data || [];
    const match = artists.find(a => normalise(a.name) === target)
      ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || target.includes(n); });
    const url = match?.picture_xl || match?.picture_big || null;
    if (url) return url;
  }

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

// ── artist_metadata helpers ───────────────────────────────────────────────────
// Persist a verified era correction so future enrichment runs don't re-correct
// the same artist. Fire-and-forget — callers don't need to await this.
function persistEraFix(name, era) {
  if (!supabase) return;
  const slug = makeCacheKey([name]);
  supabase
    .from("artist_metadata")
    .upsert(
      { slug, name, era, era_verified: true, updated_at: new Date().toISOString() },
      { onConflict: "slug" }
    )
    .then(() => {})
    .catch(err => console.warn(`[artist_metadata] write failed for ${name}:`, err.message));
}

// Batch-fetch all verified eras for a list of artist names.
// Returns a Map<lowerCaseName, era> — only includes artists with era_verified=true.
async function getVerifiedEras(names) {
  if (!supabase || !names?.length) return new Map();
  const slugs = [...new Set(names.map(n => makeCacheKey([n])))];
  const { data } = await supabase
    .from("artist_metadata")
    .select("name, era")
    .in("slug", slugs)
    .eq("era_verified", true);
  if (!data?.length) return new Map();
  return new Map(data.map(r => [r.name.toLowerCase(), r.era]));
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
        const r = await lfFetch(url);
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
  'recommend':          30  * 24 * 60 * 60 * 1000,
  'real-pool':          7   * 24 * 60 * 60 * 1000,
  'genre-spotlight':    180 * 24 * 60 * 60 * 1000,
  'genre-deeper':       180 * 24 * 60 * 60 * 1000,
  'time-machine': 14 * 24 * 60 * 60 * 1000,
  'artist-tracks':      90  * 24 * 60 * 60 * 1000,
  'artist-tracks-apple':90  * 24 * 60 * 60 * 1000,
  'artist-meta':        180 * 24 * 60 * 60 * 1000,
  'similar-artists':    180 * 24 * 60 * 60 * 1000,
  'similar-of':         180 * 24 * 60 * 60 * 1000,
  'genre-artists':      180 * 24 * 60 * 60 * 1000,
};

// Strip previewUrl from any tracks in a result — applied before both DB persistence
// and HTTP responses. Third-party CDN preview URLs must only be resolved on-demand via /api/preview.
function stripPreviewUrls(result) {
  if (!result?.tracks) return result;
  return { ...result, tracks: result.tracks.map(({ previewUrl, ...rest }) => rest) };
}

async function storeCache(cacheKey, endpoint, result, artistPool = null) {
  if (!supabase) return;
  const sanitized = stripPreviewUrls(result);
  const ttlMs = CACHE_TTL[endpoint];
  const expires_at = ttlMs ? new Date(Date.now() + ttlMs).toISOString() : null;
  await supabase.from("recommendation_cache").upsert(
    { cache_key: cacheKey, endpoint, result: sanitized, artist_pool: artistPool, expires_at },
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
      const withDeezerStripped = withDeezer.map(({ previewUrl, ...rest }) => rest);
      artistTracksMemCache.set(cacheKey, { tracks: withDeezerStripped, cachedAt: Date.now() });
      storeCache(cacheKey, "artist-tracks-apple", { tracks: withDeezerStripped }).catch(() => {});
      return { artist, tracks: withDeezerStripped };
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
  // Sort deterministically: rerankScore (per-query reranked) beats raw match
  // score, then alphabetical by name as tiebreaker. Callers that never rerank
  // fall through to the match-score path automatically.
  const sorted = [...arr].sort((a, b) => {
    const aScore = typeof a.rerankScore === 'number' ? a.rerankScore : (a.match || 0);
    const bScore = typeof b.rerankScore === 'number' ? b.rerankScore : (b.match || 0);
    const diff = bScore - aScore;
    return diff !== 0 ? diff : (a.name || '').localeCompare(b.name || '');
  });

  // Group by region
  const byRegion = {};
  for (const a of sorted) {
    const r = regionOf(a);
    (byRegion[r] = byRegion[r] || []).push(a);
  }

  // Sort regions deterministically: most candidates first, then alphabetical
  const regions = Object.keys(byRegion).sort((a, b) => {
    const diff = byRegion[b].length - byRegion[a].length;
    return diff !== 0 ? diff : a.localeCompare(b);
  });

  const chosen = [];
  const usedCountries = new Set();

  // Round-robin across regions — each pass picks the best remaining candidate per region
  while (chosen.length < n) {
    let added = false;
    for (const region of regions) {
      if (chosen.length >= n) break;
      const candidates = byRegion[region].filter(a => !usedCountries.has(a.country || a.countryCode));
      if (candidates.length === 0) continue;
      const pick = candidates[0]; // already sorted by match score
      chosen.push(pick);
      usedCountries.add(pick.country || pick.countryCode);
      added = true;
    }
    // Safety: if a full pass added nothing, break to avoid infinite loop
    if (!added) break;
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
const DISCOGS_UA   = "MusicalPassport/1.0 (musicalpassportapp@gmail.com)";

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
  return new Promise((resolve, reject) => {
    discogsQueue.push({ url, resolve, reject });
    if (!discogsBusy) drainDiscogsQueue();
  });
}

const DISCOGS_TIMEOUT_MS = 5000;
const DISCOGS_GAP_MS = 1200;
const discogsQueue = [];
let discogsBusy = false;

async function drainDiscogsQueue() {
  discogsBusy = true;
  while (discogsQueue.length > 0) {
    const { url, resolve, reject } = discogsQueue.shift();
    const token = process.env.DISCOGS_TOKEN;
    const headers = { "User-Agent": DISCOGS_UA };
    if (token) headers["Authorization"] = `Discogs token=${token}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), DISCOGS_TIMEOUT_MS);
    try {
      const response = await fetch(url, { headers, signal: controller.signal });
      resolve(response);
    } catch (e) {
      if (e.name === "AbortError") {
        console.warn(`[Discogs] Request timed out: ${url}`);
        resolve({ ok: false, status: 408 });
      } else {
        reject(e);
      }
    } finally {
      clearTimeout(timer);
    }
    if (discogsQueue.length > 0) await new Promise(r => setTimeout(r, DISCOGS_GAP_MS));
  }
  discogsBusy = false;
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
const MB_UA   = "MusicalPassport/1.0 (musicalpassportapp@gmail.com)";

// Rate-limit queue: MusicBrainz allows 1 req/sec. Keep one global queue for all
// user/background work so requests never hit MB concurrently.
const MB_TIMEOUT_MS = 15000;
const MB_REQUEST_GAP_MS = 1500;
const MB_MAX_RETRIES = 2;
const MB_RETRY_BASE_MS = 8000;    // fallback backoff when Retry-After is absent/zero
const MB_MAX_RETRY_AFTER_MS = 30000;
const mbQueue = [];
let mbBusy = false;
let mbLastRequestAt = 0; // timestamp of the last fired request (including retries)
// When a 503 is received, stamp the earliest time the next request may fire.
// This persists across item boundaries so the queue doesn't charge straight
// into a rate-limited MB after finishing the retried item.
let mbRateLimitedUntil = 0;

// Circuit breaker: after MB_CIRCUIT_THRESHOLD consecutive transient failures
// (503/429/5xx/timeout) across queue items, open the circuit for
// MB_CIRCUIT_OPEN_MS. While open, every queued mbFetch resolves immediately
// with { ok:false, status:503 } — no network request fires. Callers already
// handle this via isMbTransientStatus → null fallback.
const MB_CIRCUIT_THRESHOLD  = 3;
const MB_CIRCUIT_OPEN_MS    = 5 * 60 * 1000; // 5 minutes
let mbConsecutiveFailures   = 0;
let mbCircuitOpenUntil      = 0;

async function logMbHttpIssue(url, response, elapsedMs) {
  const headers = {
    retryAfter: response.headers.get("retry-after") || null,
    contentType: response.headers.get("content-type") || null,
    server: response.headers.get("server") || null,
  };

  let bodySnippet = "";
  try {
    bodySnippet = (await response.clone().text()).replace(/\s+/g, " ").trim().slice(0, 240);
  } catch {
    bodySnippet = "";
  }

  const details = [
    `status=${response.status}`,
    response.statusText ? `statusText=${JSON.stringify(response.statusText)}` : null,
    `elapsedMs=${elapsedMs}`,
    headers.retryAfter ? `retryAfter=${headers.retryAfter}` : null,
    headers.contentType ? `contentType=${JSON.stringify(headers.contentType)}` : null,
    headers.server ? `server=${JSON.stringify(headers.server)}` : null,
  ].filter(Boolean).join(" ");

  console.warn(`[MB] HTTP issue: ${url} ${details}`);
  if (bodySnippet) console.warn(`[MB] Response body: ${bodySnippet}`);
  if (response.status === 503) {
    console.warn("[MB] 503 from MusicBrainz. Per MB docs, throttling/load shedding is returned as 503 Service Unavailable.");
  }
}

function mbFetch(url) {
  return new Promise((resolve, reject) => {
    mbQueue.push({ url, resolve, reject });
    if (!mbBusy) drainMbQueue();
  });
}

function parseRetryAfterMs(value) {
  if (!value) return null;
  const seconds = Number(value);
  // Treat 0 as "no meaningful hint" so the backoff formula kicks in instead.
  // MB sends Retry-After: 0 when it can't compute a real delay — not as "retry now".
  if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000;
  const at = Date.parse(value);
  if (Number.isNaN(at)) return null;
  return Math.max(0, at - Date.now());
}

async function drainMbQueue() {
  mbBusy = true;
  while (mbQueue.length > 0) {
    const { url, resolve, reject } = mbQueue.shift();

    // ── Circuit breaker: if open, short-circuit without hitting the network ──
    if (Date.now() < mbCircuitOpenUntil) {
      const remainingS = Math.ceil((mbCircuitOpenUntil - Date.now()) / 1000);
      console.warn(`[MB] circuit open — skipping queued call (${remainingS}s remaining, ${mbQueue.length} more queued)`);
      resolve({ ok: false, status: 503 });
      continue;
    }

    try {
      let finalResponse = null;
      for (let attempt = 0; attempt <= MB_MAX_RETRIES; attempt++) {
        // Enforce minimum gap before every fire — including retries and queue-idle → refill.
        // The old bottom-of-loop gap only ran between consecutive queued items, so requests
        // arriving after an idle period fired immediately and could exceed 1 req/sec.
        const sinceLastMs = Date.now() - mbLastRequestAt;
        const gapNeeded = Math.max(0, MB_REQUEST_GAP_MS - sinceLastMs, mbRateLimitedUntil - Date.now());
        if (gapNeeded > 0) await new Promise(r => setTimeout(r, gapNeeded));
        mbLastRequestAt = Date.now();

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), MB_TIMEOUT_MS);
        const startedAt = Date.now();
        try {
          const r = await fetch(url, {
            headers: { "User-Agent": MB_UA, Accept: "application/json" },
            signal: controller.signal,
          });
          const elapsedMs = Date.now() - startedAt;
          if (!r.ok) await logMbHttpIssue(url, r, elapsedMs);
          if (r.ok || !isMbTransientStatus(r.status) || attempt === MB_MAX_RETRIES) {
            finalResponse = r;
            break;
          }

          const retryAfterMs = parseRetryAfterMs(r.headers.get("retry-after"));
          const backoffMs = Math.min(
            retryAfterMs ?? MB_RETRY_BASE_MS * (attempt + 1),
            MB_MAX_RETRY_AFTER_MS
          );
          const actualWaitMs = Math.max(backoffMs, MB_REQUEST_GAP_MS);
          // Stamp the global cooldown so subsequent queue items don't fire into a
          // still-rate-limited MB immediately after this item finishes its retries.
          mbRateLimitedUntil = Math.max(mbRateLimitedUntil, Date.now() + actualWaitMs);
          console.warn(
            `[MB] Retrying after transient HTTP ${r.status} in ${actualWaitMs}ms (attempt ${attempt + 1}/${MB_MAX_RETRIES}): ${url}`
          );
          await new Promise(r => setTimeout(r, actualWaitMs));
          continue;
        } catch (e) {
          if (e.name === "AbortError") {
            const elapsedMs = Date.now() - startedAt;
            console.warn(
              `[MB] Request timed out after ${elapsedMs}ms (timeout=${MB_TIMEOUT_MS}ms, queued=${mbQueue.length}, attempt=${attempt + 1}/${MB_MAX_RETRIES + 1}): ${url}`
            );
            if (attempt === MB_MAX_RETRIES) {
              finalResponse = { ok: false, status: 408 };
              break;
            }
            const backoffMs = MB_RETRY_BASE_MS * (attempt + 1);
            console.warn(`[MB] Retrying after timeout in ${backoffMs}ms: ${url}`);
            await new Promise(r => setTimeout(r, Math.max(backoffMs, MB_REQUEST_GAP_MS)));
            continue;
          }
          throw e;
        } finally {
          clearTimeout(timer);
        }
      }

      // ── Circuit breaker bookkeeping ──
      const succeeded = finalResponse?.ok === true;
      if (succeeded) {
        if (mbConsecutiveFailures > 0) mbConsecutiveFailures = 0;
      } else {
        mbConsecutiveFailures++;
        if (mbConsecutiveFailures >= MB_CIRCUIT_THRESHOLD && Date.now() >= mbCircuitOpenUntil) {
          mbCircuitOpenUntil = Date.now() + MB_CIRCUIT_OPEN_MS;
          console.warn(
            `[MB] circuit OPEN after ${mbConsecutiveFailures} consecutive failures — suspending MB requests for ${MB_CIRCUIT_OPEN_MS / 1000}s (${mbQueue.length} queued calls will be skipped)`
          );
        }
      }

      resolve(finalResponse ?? { ok: false, status: 520 });
    } catch (e) {
      mbConsecutiveFailures++;
      if (mbConsecutiveFailures >= MB_CIRCUIT_THRESHOLD && Date.now() >= mbCircuitOpenUntil) {
        mbCircuitOpenUntil = Date.now() + MB_CIRCUIT_OPEN_MS;
        console.warn(`[MB] circuit OPEN after ${mbConsecutiveFailures} consecutive failures (exception: ${e.message})`);
      }
      reject(e);
    }
    // Gap is now enforced at the top of each fire — no bottom-of-loop wait needed.
  }
  mbBusy = false;
}

// In-memory cache for MB artist → ISO country (avoids repeat lookups within a process lifetime)
const mbArtistCache = new Map(); // artistName → { country: string|null, at: number }
const MB_ARTIST_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days
const MB_TRANSIENT_CACHE_TTL = 60 * 60 * 1000; // 1 hour for timeouts / temporary failures
const mbTransientCache = new Map(); // key → { value, at }

function getMbTransientCached(key) {
  const cached = mbTransientCache.get(key);
  if (!cached) return undefined;
  if (Date.now() - cached.at >= MB_TRANSIENT_CACHE_TTL) {
    mbTransientCache.delete(key);
    return undefined;
  }
  return cached.value;
}

function setMbTransientCached(key, value = null) {
  mbTransientCache.set(key, { value, at: Date.now() });
}

function isMbTransientStatus(status) {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

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

// Batched variant — one Supabase IN-query instead of N single-row lookups.
// Returns a Map<normalizedName, country_code|null> containing ONLY entries that
// are known (either memory-cached or found in artist_countries). Missing keys
// indicate "not yet looked up" and should fall through to mbArtistCountry.
async function getMbArtistCachedBatch(artistNames) {
  const result = new Map();
  const slugToKey = new Map();
  const needFromDb = [];

  for (const name of artistNames) {
    const key = name.toLowerCase().trim();
    if (!key || result.has(key)) continue;
    const mem = mbArtistCache.get(key);
    if (mem && Date.now() - mem.at < MB_ARTIST_CACHE_TTL) {
      result.set(key, mem.country ?? null);
      continue;
    }
    const slug = makeCacheKey([key]);
    slugToKey.set(slug, key);
    needFromDb.push(slug);
  }

  if (supabase && needFromDb.length > 0) {
    const { data } = await supabase
      .from("artist_countries")
      .select("artist_slug, country_code")
      .in("artist_slug", needFromDb);
    for (const row of data || []) {
      const key = slugToKey.get(row.artist_slug);
      if (!key) continue;
      mbArtistCache.set(key, { country: row.country_code, at: Date.now() });
      result.set(key, row.country_code ?? null);
    }
  }

  return result;
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
  // A
  "Afghanistan":"AF","Albania":"AL","Algeria":"DZ","Angola":"AO","Argentina":"AR",
  "Armenia":"AM","Australia":"AU","Austria":"AT","Azerbaijan":"AZ",
  // B
  "Bahrain":"BH","Bangladesh":"BD","Barbados":"BB","Belarus":"BY","Belgium":"BE",
  "Belize":"BZ","Benin":"BJ","Bhutan":"BT","Bolivia":"BO","Bosnia":"BA","Botswana":"BW",
  "Brazil":"BR","Brunei":"BN","Bulgaria":"BG","Burkina Faso":"BF","Burundi":"BI",
  // C
  "Cambodia":"KH","Cameroon":"CM","Canada":"CA","Cape Verde":"CV",
  "Central African Republic":"CF","Chad":"TD","Chile":"CL","China":"CN",
  "Colombia":"CO","Comoros":"KM","Congo":"CG","DR Congo":"CD","Costa Rica":"CR",
  "Croatia":"HR","Cuba":"CU","Cyprus":"CY","Czechia":"CZ","Czech Republic":"CZ",
  // D
  "Denmark":"DK","Djibouti":"DJ","Dominica":"DM","Dominican Republic":"DO",
  // E
  "East Timor":"TL","Ecuador":"EC","Egypt":"EG","El Salvador":"SV",
  "England":"GB","Equatorial Guinea":"GQ","Eritrea":"ER","Estonia":"EE","Eswatini":"SZ",
  "Ethiopia":"ET",
  // F
  "Fiji":"FJ","Finland":"FI","France":"FR",
  // G
  "Gabon":"GA","Gambia":"GM","Georgia":"GE","Germany":"DE","Ghana":"GH",
  "Greece":"GR","Grenada":"GD","Guadeloupe":"GP","Guatemala":"GT","Guinea":"GN",
  "Guinea-Bissau":"GW","Guyana":"GY",
  // H
  "Haiti":"HT","Honduras":"HN","Hong Kong":"HK","Hungary":"HU",
  // I
  "Iceland":"IS","India":"IN","Indonesia":"ID","Iran":"IR","Iraq":"IQ",
  "Ireland":"IE","Israel":"IL","Italy":"IT","Ivory Coast":"CI",
  // J
  "Jamaica":"JM","Japan":"JP","Jordan":"JO",
  // K
  "Kazakhstan":"KZ","Kenya":"KE","Kiribati":"KI","Kosovo":"XK","Kuwait":"KW",
  "Kyrgyzstan":"KG",
  // L
  "Laos":"LA","Latvia":"LV","Lebanon":"LB","Lesotho":"LS","Liberia":"LR",
  "Libya":"LY","Liechtenstein":"LI","Lithuania":"LT","Luxembourg":"LU",
  // M
  "Madagascar":"MG","Malawi":"MW","Malaysia":"MY","Maldives":"MV","Mali":"ML","Malta":"MT",
  "Marshall Islands":"MH","Martinique":"MQ","Mauritania":"MR","Mauritius":"MU","Mexico":"MX",
  "Micronesia":"FM","Moldova":"MD","Monaco":"MC","Mongolia":"MN","Montenegro":"ME","Morocco":"MA","Mozambique":"MZ",
  "Myanmar":"MM",
  // N
  "Namibia":"NA","Nauru":"NR","Nepal":"NP","Netherlands":"NL","New Zealand":"NZ",
  "Nicaragua":"NI","Niger":"NE","Nigeria":"NG","North Korea":"KP","North Macedonia":"MK","Norway":"NO",
  // O
  "Oman":"OM",
  // P
  "Pakistan":"PK","Palau":"PW","Palestine":"PS","Panama":"PA","Papua New Guinea":"PG",
  "Paraguay":"PY","Peru":"PE","Philippines":"PH","Poland":"PL","Portugal":"PT",
  "Puerto Rico":"PR",
  // Q
  "Qatar":"QA",
  // R
  "Romania":"RO","Russia":"RU","Rwanda":"RW","Réunion":"RE",
  // S
  "Samoa":"WS","San Marino":"SM","Sao Tome & Principe":"ST","Saudi Arabia":"SA","Scotland":"GB","Senegal":"SN","Serbia":"RS",
  "Seychelles":"SC","Sierra Leone":"SL","Singapore":"SG","Slovakia":"SK",
  "Slovenia":"SI","Solomon Islands":"SB","Somalia":"SO","South Africa":"ZA",
  "South Korea":"KR","South Sudan":"SS","Spain":"ES","Sri Lanka":"LK","Sudan":"SD","Suriname":"SR",
  "Sweden":"SE","Switzerland":"CH","Syria":"SY",
  // T
  "Taiwan":"TW","Tajikistan":"TJ","Tanzania":"TZ","Thailand":"TH","Togo":"TG",
  "Tonga":"TO","Trinidad & Tobago":"TT","Tunisia":"TN","Turkey":"TR",
  "Turkmenistan":"TM","Tuvalu":"TV","Timor-Leste":"TL",
  // U
  "UAE":"AE","Uganda":"UG","Ukraine":"UA","United Kingdom":"GB","Uruguay":"UY","USA":"US","Uzbekistan":"UZ",
  // V
  "Vanuatu":"VU","Vatican City":"VA","Venezuela":"VE","Vietnam":"VN",
  // W
  "Wales":"GB",
  // Y
  "Yemen":"YE",
  // Z
  "Zambia":"ZM","Zimbabwe":"ZW",
};

// ── Streaming floor helpers ───────────────────────────────
const DECADES_LIST = ["1900s","1910s","1920s","1930s","1940s","1950s","1960s","1970s","1980s","1990s","2000s","2010s","2020s"];
function yearToDecade(year) {
  const y = parseInt(year);
  if (isNaN(y) || y < 1900) return null;
  return `${Math.floor(y / 10) * 10}s`;
}
function decadeIndex(decade) { return DECADES_LIST.indexOf(decade); }
function normalizeDecade(decade) {
  if (!decade) return null;
  const clean = String(decade).trim();
  if (DECADES_LIST.includes(clean)) return clean;
  const yearMatch = clean.match(/\b(19|20)\d0s\b/);
  return yearMatch && DECADES_LIST.includes(yearMatch[0]) ? yearMatch[0] : null;
}
function nextDecade(decade) {
  const idx = decadeIndex(normalizeDecade(decade));
  return idx >= 0 && idx < DECADES_LIST.length - 1 ? DECADES_LIST[idx + 1] : null;
}

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

async function maybeRaiseStreamingFloorAfterEmptyDecade(country, attemptedDecade, currentFloor, existingCount, addedCount) {
  const normalizedAttempt = normalizeDecade(attemptedDecade);
  const normalizedFloor = normalizeDecade(currentFloor);
  if (!country || !normalizedAttempt || !normalizedFloor) return false;
  if (existingCount > 0 || addedCount > 0) return false;
  if (normalizedAttempt !== normalizedFloor) return false;

  const bumped = nextDecade(normalizedFloor);
  if (!bumped || bumped === normalizedFloor) return false;

  await patchStreamingFloor(country, bumped);
  console.log(`[enrich-decade] ${country} ${normalizedAttempt}: no viable artists found, raised streaming floor → ${bumped}`);
  return true;
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

// Extract an ISO country code from a genre string when it explicitly names a country/region.
// Used as a fallback when MusicBrainz can't resolve an artist. Uses word-boundary
// matching so "reggae"→JM does NOT falsely fire on "reggaeton" (a Puerto Rican /
// Colombian genre), and "samba"→BR doesn't fire on "samba de roda" variants from
// other countries.
const GENRE_COUNTRY_HINTS = [
  ['south african', 'ZA'], ['amapiano', 'ZA'], ['kwaito', 'ZA'], ['afro house', 'ZA'],
  ['nigerian', 'NG'], ['afrobeats', 'NG'], ['afropop', 'NG'], ['naija', 'NG'],
  ['ghanaian', 'GH'], ['highlife', 'GH'],
  ['grime', 'GB'], ['uk rap', 'GB'], ['uk hip-hop', 'GB'], ['british hip hop', 'GB'], ['afroswing', 'GB'],
  ['k-pop', 'KR'], ['k-rap', 'KR'], ['k-hip-hop', 'KR'], ['korean hip', 'KR'], ['korean r&b', 'KR'],
  ['j-pop', 'JP'], ['j-rock', 'JP'], ['j-rap', 'JP'], ['japanese hip', 'JP'],
  ['french rap', 'FR'], ['french hip-hop', 'FR'], ['french pop', 'FR'],
  ['german rap', 'DE'], ['deutschrap', 'DE'],
  ['australian hip', 'AU'], ['aussie hip', 'AU'], ['australian pop', 'AU'],
  ['brazilian', 'BR'], ['baile funk', 'BR'], ['funk carioca', 'BR'], ['pagode', 'BR'], ['samba', 'BR'],
  ['jamaican', 'JM'], ['dancehall', 'JM'], ['reggae', 'JM'],
  ['cuban', 'CU'], ['son cubano', 'CU'], ['timba', 'CU'],
  ['turkish', 'TR'], ['arabesque', 'TR'],
  ['congolese', 'CD'], ['soukous', 'CD'],
  ['ethiopian', 'ET'],
];
// Pre-compile word-boundary regexes for each hint. The character-class custom
// boundary handles hyphens and apostrophes (e.g. "k-pop", "j-rock") since \b
// treats them as word boundaries anyway; we just need to prevent "reggae" from
// matching inside "reggaeton".
const GENRE_COUNTRY_HINT_REGEXES = GENRE_COUNTRY_HINTS.map(([kw, code]) => {
  const escaped = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // (^|[^a-z0-9]) and ([^a-z0-9]|$) — require non-alphanumeric boundary so
  // "reggae" doesn't match the "reggae" prefix of "reggaeton".
  return { re: new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, 'i'), code };
});

function countryHintFromGenre(genre) {
  if (!genre) return null;
  const g = genre.toLowerCase();
  for (const { re, code } of GENRE_COUNTRY_HINT_REGEXES) {
    if (re.test(g)) return code;
  }
  return null;
}

// Verify and correct country info for each artist in a pool using MusicBrainz.
// Runs lookups in the existing MB queue (rate-limited to 1/sec), so it's slow but accurate.
// Conservative by default: only fill missing country data. Similar-artist pools
// can opt into overwrites because Claude country labels are untrusted there.
// Sets mbVerified:true when MB confirms, mbVerified:'genre' when genre hint is used,
// leaves mbVerified unset (falsy) when neither source can confirm.
async function verifyPoolCountries(pool, { overwriteExisting = false } = {}) {
  return Promise.all(pool.map(async (artist) => {
    // Artists already confirmed by MusicBrainz are authoritative — never re-query them.
    if (artist.mbVerified === true) return artist;
    // Without overwrite, also skip anything that already has a country code.
    if (artist.countryCode && !overwriteExisting) return artist;
    const mbCode = await mbArtistCountry(artist.name);
    if (mbCode) {
      const mbName = ISO_TO_COUNTRY[mbCode];
      if (!mbName) return artist; // unknown ISO code, keep existing
      if (artist.countryCode && artist.countryCode !== mbCode) {
        console.log(`[verify-country] ${artist.name}: corrected ${artist.countryCode} → ${mbCode}(${mbName})`);
      } else if (!artist.countryCode) {
        console.log(`[verify-country] ${artist.name}: filled ${mbCode}(${mbName})`);
      }
      return { ...artist, country: mbName, countryCode: mbCode, mbVerified: true };
    }
    // MB returned null — try genre-based country hint as a secondary signal
    const genreCode = countryHintFromGenre(artist.genre);
    if (genreCode && ISO_TO_COUNTRY[genreCode] && (!artist.mbVerified || overwriteExisting)) {
      if (artist.countryCode && artist.countryCode !== genreCode) {
        console.log(`[verify-country] ${artist.name}: genre-hint corrected ${artist.countryCode} → ${genreCode}(${ISO_TO_COUNTRY[genreCode]})`);
      }
      return { ...artist, country: ISO_TO_COUNTRY[genreCode], countryCode: genreCode, mbVerified: 'genre' };
    }
    return artist; // could not verify — keep existing data as-is
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
  const transientKey = `country:${artistName.toLowerCase().trim()}`;
  const transient = getMbTransientCached(transientKey);
  if (transient !== undefined) return transient;
  const cached = await getMbArtistCached(artistName);
  if (cached !== undefined) return cached?.trim() ?? null;
  try {
    const searchName = canonicalizeArtistName(artistName);
    const target = normalizeArtistKey(searchName);
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(searchName)}&limit=5&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) {
      if (isMbTransientStatus(r.status)) {
        setMbTransientCached(transientKey, null);
        return null;
      }
      await setMbArtistCached(artistName, null);
      return null;
    }
    const d = await r.json();
    const candidates = (d.artists || []).filter(a => {
      const score = a.score || 0;
      if (score < 80) return false;
      const candidate = normalizeArtistKey(a.name || "");
      return candidate === target || candidate.includes(target) || (candidate.length >= 6 && target.includes(candidate)) || fuzzyArtistMatch(candidate, target);
    });

    // Disambiguation: if multiple high-score matches disagree on country, the
    // name is ambiguous (e.g. UK folk legend "Nick Drake" vs. Canadian musician
    // "Nick Drake", or the many "Rihanna"s in MB). Return null rather than
    // picking arbitrarily — pollution of artist_countries with a wrong code
    // breaks both diversity rules and the directPool fast path. The caller
    // (verifyPoolCountries) will fall back to genre-hint or keep Claude's code.
    const countryCodes = [...new Set(
      candidates.map(a => a.country?.trim()).filter(Boolean)
    )];
    if (countryCodes.length > 1) {
      console.log(`[mb-disambig] "${artistName}" has ${countryCodes.length} country candidates (${countryCodes.join(', ')}) — leaving unverified`);
      await setMbArtistCached(artistName, null);
      return null;
    }

    const country = countryCodes[0] ?? null;
    await setMbArtistCached(artistName, country);
    return country;
  } catch { return null; }
}

// ── ListenBrainz fallback for artist tracks ───────────────
// ── Last.fm fetch queue ───────────────────────────────────
// Last.fm has no hard rate limit but ~5 req/s is the community guideline.
// 300ms between calls (~3.3/s) keeps us well within that.
const LF_TIMEOUT_MS = 6000;
const lfQueue = [];
let lfBusy = false;
function lfFetch(url) {
  return new Promise((resolve, reject) => {
    lfQueue.push({ url, resolve, reject });
    if (!lfBusy) drainLfQueue();
  });
}
async function drainLfQueue() {
  lfBusy = true;
  while (lfQueue.length > 0) {
    const { url, resolve, reject } = lfQueue.shift();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), LF_TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": MB_UA },
        signal: controller.signal,
      });
      resolve(r);
    } catch (e) {
      if (e.name === "AbortError") {
        console.warn(`[LF] Request timed out: ${url}`);
        resolve({ ok: false, status: 408 });
      } else {
        reject(e);
      }
    } finally {
      clearTimeout(timer);
    }
    if (lfQueue.length > 0) await new Promise(r => setTimeout(r, 300));
  }
  lfBusy = false;
}

// ── ListenBrainz fetch queue ──────────────────────────────
// ListenBrainz has no published rate limit but is a community project.
// 500ms between calls (~2/s) is respectful.
const LB_BASE = "https://api.listenbrainz.org/1";
const LB_TIMEOUT_MS = 8000;
const lbQueue = [];
let lbBusy = false;
function lbFetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    lbQueue.push({ url, options, resolve, reject });
    if (!lbBusy) drainLbQueue();
  });
}
async function drainLbQueue() {
  lbBusy = true;
  while (lbQueue.length > 0) {
    const { url, options, resolve, reject } = lbQueue.shift();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), LB_TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        ...options,
        headers: { "User-Agent": MB_UA, ...(options.headers || {}) },
        signal: controller.signal,
      });
      resolve(r);
    } catch (e) {
      if (e.name === "AbortError") {
        console.warn(`[LB] Request timed out: ${url}`);
        resolve({ ok: false, status: 408 });
      } else {
        reject(e);
      }
    } finally {
      clearTimeout(timer);
    }
    if (lbQueue.length > 0) await new Promise(r => setTimeout(r, 500));
  }
  lbBusy = false;
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
      .slice(0, 80);
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
  // Check Supabase cache before hitting LB + MB rate-limited queues.
  // 7-day TTL — artist origin data is stable and rarely changes.
  const realPoolCacheKey = makeCacheKey(['real-pool', country]);
  const realPoolCached = await getCached(realPoolCacheKey);
  if (realPoolCached?.artist_pool?.length > 0) {
    console.log(`[real-pool] ${country}: Supabase cache hit (${realPoolCached.artist_pool.length} artists)`);
    return realPoolCached.artist_pool;
  }

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

  const result = pool.slice(0, 80);
  console.log(`[real-pool] ${country}: ${result.filter(a => a.confidence === "high").length} high-conf, ${result.filter(a => a.confidence !== "high").length} medium-conf artists`);
  // Persist so the next cold request avoids re-querying LB + MB queues
  storeCache(realPoolCacheKey, 'real-pool', {}, result).catch(() => {});
  return result;
}

// Simple in-memory cache for artist MBIDs
const mbidCache = new Map(); // artistName → { mbid, at }
const MBID_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days

async function getMbArtistMBID(artistName) {
  const transientKey = `mbid:${artistName.toLowerCase().trim()}`;
  const transient = getMbTransientCached(transientKey);
  if (transient !== undefined) return transient;
  const cached = mbidCache.get(artistName);
  if (cached && Date.now() - cached.at < MBID_CACHE_TTL) return cached.mbid;
  try {
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(artistName)}&limit=3&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) {
      if (isMbTransientStatus(r.status)) {
        setMbTransientCached(transientKey, null);
        return null;
      }
      mbidCache.set(artistName, { mbid: null, at: Date.now() });
      return null;
    }
    const d = await r.json();
    const normKey = s => (s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const target  = normKey(artistName);
    const top = (d.artists || []).find(a => {
      if ((a.score || 0) < 80) return false;
      const n = normKey(a.name || "");
      if (n === target) return true;               // exact match
      if (n.includes(target)) return true;         // MB name contains query
      // Query contains MB name — only accept if MB name is ≥ 60% of query length.
      // Blocks short tokens like "sting" (5/15 = 33%) matching "NautenStingBand",
      // while still accepting "beatles" (7/10 = 70%) matching "thebeatles".
      if (target.includes(n) && n.length >= Math.ceil(target.length * 0.6)) return true;
      return false;
    });
    const mbid = top?.id ?? null;
    mbidCache.set(artistName, { mbid, at: Date.now() });
    return mbid;
  } catch { return null; }
}

async function mbArtistDetails(artistName) {
  const searchName = canonicalizeArtistName(artistName);
  const transientKey = `details:${searchName.toLowerCase()}`;
  const transient = getMbTransientCached(transientKey);
  if (transient !== undefined) return transient;
  const target = normalizeArtistKey(searchName);
  try {
    const url = `${MB_BASE}/artist?query=artist:${encodeURIComponent(searchName)}&limit=5&fmt=json`;
    const r = await mbFetch(url);
    if (!r.ok) {
      if (isMbTransientStatus(r.status)) setMbTransientCached(transientKey, null);
      return null;
    }
    const d = await r.json();
    const artists = d.artists || [];
    const matched = artists.find(a => normalizeArtistKey(a.name || "") === target)
      ?? artists.find(a => {
        const n = normalizeArtistKey(a.name || "");
        return n === target || n.includes(target) || target.includes(n);
      })
      ?? artists.find(a => (a.score || 0) >= 85);
    if (!matched) return null;
    const beginYear = parseInt((matched["life-span"]?.begin || "").slice(0, 4), 10);
    const endYear = parseInt((matched["life-span"]?.end || "").slice(0, 4), 10);
    return {
      mbid: matched.id || null,
      canonicalName: matched.name || searchName,
      countryCode: matched.country || null,
      beginYear: Number.isFinite(beginYear) ? beginYear : null,
      endYear: Number.isFinite(endYear) ? endYear : null,
      type: matched.type || null,
      disambiguation: matched.disambiguation || null,
    };
  } catch {
    return null;
  }
}

async function discogsArtistReleaseYears(artistName, limit = 25) {
  if (!process.env.DISCOGS_TOKEN) return [];
  const searchName = canonicalizeArtistName(artistName);
  const target = normalizeArtistKey(searchName);
  try {
    const searchUrl = `${DISCOGS_BASE}/database/search?artist=${encodeURIComponent(searchName)}&type=master&per_page=${Math.min(limit, 25)}&page=1`;
    const searchRes = await discogsFetch(searchUrl);
    if (!searchRes.ok) return [];
    const searchData = await searchRes.json();
    const years = [];
    for (const result of (searchData.results || [])) {
      const year = parseInt(result.year, 10);
      if (!Number.isFinite(year) || year < 1900 || year > 2035) continue;
      const titleNorm = normalizeArtistKey(result.title || "");
      if (!titleNorm || (!titleNorm.startsWith(target) && !titleNorm.includes(target))) continue;
      years.push(year);
    }
    return [...new Set(years)].sort((a, b) => a - b).slice(0, limit);
  } catch {
    return [];
  }
}

function inferArtistEraFromEvidence({ discogsYears = [], beginYear = null, endYear = null }) {
  const yearCounts = new Map();
  for (const year of discogsYears) {
    const decade = yearToDecade(year);
    if (!decade) continue;
    yearCounts.set(decade, (yearCounts.get(decade) || 0) + 1);
  }
  const ranked = [...yearCounts.entries()].sort((a, b) => b[1] - a[1] || decadeIndex(a[0]) - decadeIndex(b[0]));
  if (ranked[0]?.[0] && ranked[0][1] >= 2) return { era: ranked[0][0], confidence: "high" };
  if (ranked[0]?.[0]) return { era: ranked[0][0], confidence: "medium" };
  const mbYear = beginYear || endYear;
  const mbEra = yearToDecade(mbYear);
  return mbEra ? { era: mbEra, confidence: "low" } : { era: null, confidence: "none" };
}

async function getArtistMetaEvidence(artistName) {
  const canonical = canonicalizeArtistName(artistName);
  const cacheKey = makeCacheKey(["artist-meta", canonical]);
  const cached = await getCached(cacheKey);
  if (cached?.result?.canonicalName) return cached.result;

  const correctedName = await lastfmArtistCorrection(canonical).catch(() => null) || canonical;
  const [mbDetails, discogsYears] = await Promise.all([
    mbArtistDetails(correctedName),
    discogsArtistReleaseYears(correctedName, 25), // 25 releases gives a reliable decade histogram
  ]);

  const inferred = inferArtistEraFromEvidence({
    discogsYears,
    beginYear: mbDetails?.beginYear ?? null,
    endYear: mbDetails?.endYear ?? null,
  });

  const result = {
    canonicalName: mbDetails?.canonicalName || correctedName,
    mbid: mbDetails?.mbid || null,
    countryCode: mbDetails?.countryCode || null,
    beginYear: mbDetails?.beginYear ?? null,
    endYear: mbDetails?.endYear ?? null,
    discogsYears,
    inferredEra: inferred.era,
    eraConfidence: inferred.confidence,
  };
  await storeCache(cacheKey, "artist-meta", result).catch(() => {});
  return result;
}

async function applyArtistEvidence(artists) {
  const hydrated = [];
  let droppedAmbiguous = 0;
  for (const artist of normalizeArtistNames(artists)) {
    const meta = await getArtistMetaEvidence(artist.name);
    if (isAmbiguousArtistName(artist.name) && !hasStrongArtistEvidence(meta, artist)) {
      droppedAmbiguous++;
      continue;
    }
    const correctedEra = normalizeDecade(meta?.inferredEra);
    hydrated.push({
      ...artist,
      name: meta?.canonicalName || artist.name,
      ...(correctedEra && (meta.eraConfidence === "high" || !normalizeDecade(artist.era)) ? { era: correctedEra } : {}),
    });
  }
  if (droppedAmbiguous > 0) {
    console.log(`[artist-filter] dropped ${droppedAmbiguous} ambiguous low-evidence artist(s)`);
  }
  return dedupeArtistsByName(hydrated);
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
const similarInFlight   = new Map(); // normalized artist name → Promise
const similarSwrInFlight = new Set(); // cache keys with an in-flight SWR refresh

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
        artists: pickDiverseByEra(annotateTrackStatus(pool), pool.length),
        didYouKnow: cached.result.didYouKnow,
        fromCache: true,
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
    // Split real pool into mandatory (high-conf = in both MB+LB) and supplemental
    const highConf     = realPool.filter(a => a.confidence === "high");
    const medConf      = realPool.filter(a => a.confidence !== "high");
    const mandatoryPool  = [...highConf, ...medConf].slice(0, 8);
    const suggestionPool = [...highConf, ...medConf].slice(8, 14);
    const remaining      = Math.max(0, 12 - mandatoryPool.length);

    const realPoolNote = mandatoryPool.length >= 4
      // Strong mandatory block: Claude must use these artists verbatim
      ? `\n\nMANDATORY ARTISTS — you MUST include ALL ${mandatoryPool.length} of these in your response.\nDo NOT omit, rename, or substitute any of them (they are database-verified as being from ${country}):\n${
          mandatoryPool.map(a => `- ${a.name}${a.confidence === "high" ? " ✓✓" : " ✓"}${a.tags.length ? ` [${a.tags.slice(0, 2).join(", ")}]` : ""}`).join("\n")
        }${suggestionPool.length > 0
          ? `\n\nAdditional verified ${country} artists (use if they fit your era/genre balance):\n${suggestionPool.map(a => `- ${a.name}`).join("\n")}`
          : ""}\n\n${remaining > 0
          ? `Add exactly ${remaining} more artists from ${country} to reach 12 total. Every artist must genuinely be from ${country} — do NOT invent or misattribute.`
          : "Use exactly these artists — no additions needed."}`
      // Fallback for small real pools: weaker suggestion language
      : realPool.length > 0
      ? `\n\nVERIFIED ARTISTS from MusicBrainz + ListenBrainz databases for ${country}:\n${
          realPool.map(a => `- ${a.name}${a.confidence === "high" ? " [verified in both MB+LB]" : ""}${a.tags.length ? ` (${a.tags.slice(0, 3).join(", ")})` : ""}`).join("\n")
        }\n\nThese artists are confirmed to be from ${country}. Include as many as possible — prioritize high-confidence ones. You may add artists not in this list only if you are certain they are from ${country}.`
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

    const _result = { genres: rec.genres, artists: pickDiverseByEra(artistPool, artistPool.length), didYouKnow: rec.didYouKnow };
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

// ── Decade enrichment ──────────────────────────────────────────────────────
// Called in the background when a country+decade filter returns fewer than 5
// era-matching artists. Generates more artists for that specific decade and
// merges them into the existing country pool without replacing anything.
const decadeEnrichInFlight = new Set();

app.post("/api/enrich-decade", async (req, res) => {
  const { country, decade } = req.body;
  if (!country || !decade) return res.status(400).json({ error: "Missing country or decade" });

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "No API key" });

  const inflightKey = `${country}::${decade}`;
  if (decadeEnrichInFlight.has(inflightKey)) {
    return res.json({ status: "in_progress" });
  }

  const cacheKey = makeCacheKey(["recommend", country]);
  const cached = await getCached(cacheKey);
  if (!cached?.artist_pool) return res.json({ status: "no_pool", added: 0 });

  const pool = cached.artist_pool;
  const decadeYear = decade.slice(0, 4); // "1990" from "1990s"
  const existingDecadeArtists = pool.filter(a => a.era && a.era.includes(decadeYear));

  if (existingDecadeArtists.length >= 5) {
    return res.json({ status: "sufficient", count: existingDecadeArtists.length, added: 0 });
  }

  const needed = 5 - existingDecadeArtists.length;
  console.log(`[enrich-decade] ${country} ${decade}: have ${existingDecadeArtists.length}, need ${needed} more`);

  decadeEnrichInFlight.add(inflightKey);
  // Respond immediately so the client does not block
  res.json({ status: "started", needed });

  // Do the enrichment work asynchronously after responding
  (async () => {
    try {
      const currentFloor = cached.result?.streamingFloor;
      const existingNames = pool.map(a => a.name);

      const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 800,
          system: "You are a world music curator and ethnomusicologist. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
          messages: [{
            role: "user",
            content: `Give me ${needed + 3} real artists from ${country} who were most active in the ${decade}.

Artists already in our database (do NOT include these): ${existingNames.join(", ")}

Return exactly this JSON:
{
  "artists": [
    {
      "name": "Artist Name",
      "genre": "specific genre",
      "era": "${decade}",
      "similarTo": "One well-known international artist name"
    }
  ]
}

Rules:
- Only artists genuinely from ${country} who were primarily active in the ${decade}
- era must be exactly "${decade}"
- Use exact names as they appear on streaming platforms
- Do not include any artist from the exclusion list above
- Return between ${needed} and ${needed + 3} artists`,
          }],
        }),
      });

      const aiData = await aiRes.json();
      const raw = (aiData.content?.[0]?.text || "").replace(/```json|```/g, "").trim();
      let newArtists;
      try { newArtists = JSON.parse(raw).artists || []; } catch { return; }

      newArtists = await applyArtistEvidence(newArtists);

      // Deduplicate against existing pool
      const normExisting = new Set(pool.map(a => a.name.toLowerCase().replace(/[^a-z0-9]/g, "")));
      newArtists = newArtists.filter(a => !normExisting.has(a.name.toLowerCase().replace(/[^a-z0-9]/g, "")));

      if (!newArtists.length) {
        console.log(`[enrich-decade] ${country} ${decade}: no new artists after dedup`);
        await maybeRaiseStreamingFloorAfterEmptyDecade(country, decade, currentFloor, existingDecadeArtists.length, 0);
        return;
      }

      // Filter flagged, verify tracks, fetch images — same pipeline as /api/recommend
      const unflagged = await filterOutFlaggedArtists(newArtists);
      const verified = await verifyArtistTracksForRecommend(unflagged, country);
      const verifiedNames = new Set(verified.map(a => a.name));
      const toAdd = verified.length >= 1 ? verified : unflagged.slice(0, needed);
      const withVerification = toAdd.map(a => ({ ...a, era: decade, hasVerifiedTracks: verifiedNames.has(a.name) }));

      const imageUrls = await Promise.all(
        withVerification.map(a => fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null))
      );
      const withImages = withVerification.map((a, i) => ({ ...a, imageUrl: imageUrls[i] || undefined }));

      if (!withImages.length) {
        console.log(`[enrich-decade] ${country} ${decade}: no artists survived verification`);
        await maybeRaiseStreamingFloorAfterEmptyDecade(country, decade, currentFloor, existingDecadeArtists.length, 0);
        return;
      }

      // Re-fetch latest cached pool (may have changed since we started)
      const latestCached = await getCached(cacheKey);
      const latestPool = latestCached?.artist_pool || pool;
      const latestNames = new Set(latestPool.map(a => a.name.toLowerCase().replace(/[^a-z0-9]/g, "")));
      const currentDecadeCount = latestPool.filter(a => a.era && a.era.includes(decadeYear)).length;
      const stillNeeded = Math.max(0, 5 - currentDecadeCount);
      const trulyNew = withImages
        .filter(a => !latestNames.has(a.name.toLowerCase().replace(/[^a-z0-9]/g, "")))
        .slice(0, stillNeeded);

      if (!trulyNew.length) {
        console.log(`[enrich-decade] ${country} ${decade}: all new artists already in pool`);
        await maybeRaiseStreamingFloorAfterEmptyDecade(country, decade, currentFloor, existingDecadeArtists.length, 0);
        return;
      }

      const mergedPool = [...latestPool, ...trulyNew].slice(0, 80);
      await storeCache(cacheKey, "recommend", latestCached?.result ?? cached.result, mergedPool);

      const finalCount = mergedPool.filter(a => a.era && a.era.includes(decadeYear)).length;
      console.log(`[enrich-decade] ✓ ${country} ${decade}: added ${trulyNew.length}, pool now has ${finalCount} from this decade`);

      backfillDeezerForArtists(trulyNew.map(a => a.name)).catch(() => {});
    } catch (err) {
      console.error(`[enrich-decade] error ${country} ${decade}:`, err.message);
    } finally {
      decadeEnrichInFlight.delete(inflightKey);
    }
  })();
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
        return res.json(stripPreviewUrls(tmResult));
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
        return res.json(stripPreviewUrls(tmResult));
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
        return res.json(stripPreviewUrls(tmResult));
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
        return res.json(stripPreviewUrls(tmResult));
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
    res.json(stripPreviewUrls(tmResult));
  } catch (err) {
    console.error("Time machine error:", err);
    res.status(500).json({ error: err.message });
  }
});


// ── Genre Spotlight endpoint ─────────────────────────────
app.post("/api/genre-spotlight", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { genre: rawGenre, country: rawCountry, service = "spotify", relatedArtistNames = [], seedArtist } = req.body;
  if (!rawGenre) return res.status(400).json({ error: "Missing genre." });

  const country = (rawCountry || "").trim();
  const worldwide = !country;

  const genre = await resolveGenreCanonical(rawGenre);
  if (genre !== rawGenre) console.log(`[genre-spotlight] genre normalised: "${rawGenre}" → "${genre}"`);

  const gsCacheKey = makeCacheKey(["genrespotlight", genre, worldwide ? "WORLDWIDE" : country, service]);
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
    return res.json(stripPreviewUrls(supplemented));
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
        model: worldwide ? "claude-sonnet-4-20250514" : "claude-haiku-4-5-20251001",
        max_tokens: worldwide ? 1800 : 1200,
        system: `You are a world music expert with deep knowledge of niche regional genres across Asia, Africa, the Middle East, Latin America, the Caribbean, and Europe. Return ONLY valid JSON — no markdown, no backticks, no preamble.`,
        messages: [{
          role: "user",
          content: worldwide
? `The user wants to explore "${genre}" worldwide. Provide globally representative artists and tracks across the countries where this genre is genuinely practiced.
${seedArtist ? `\nCRITICAL: "${seedArtist}" is a confirmed real artist tied to this genre. You MUST include them in the artists array and include at least one of their actual, real tracks (with their country) in the tracks array. Do not invent track titles.\n` : ""}
Return exactly this JSON:
{
  "explanation": "1 sentence on the genre's cultural roots and how/where it is practiced globally",
  "tracks": [
    { "title": "exact track title", "artist": "exact artist name", "artistCountry": "Country name", "artistCountryCode": "XX" }
  ],
  "artists": [
    { "name": "Artist Name", "country": "Country name", "countryCode": "XX" }
  ],
  "suggestedGenres": ["Genre 1", "Genre 2", "Genre 3"]
}
Rules:
- Return 6–8 tracks. Maximum 1 track per artist. Maximum 2 tracks from any single country.
- Choose countries based on where the genre is genuinely practiced:
  • Hyper-local genres (e.g. Shibuya-kei → Japan only, Mbalax → Senegal only): all tracks may be from the single home country.
  • Multi-country genres (e.g. Fado → Portugal & Cabo Verde, Reggae → Jamaica & UK): spread across those origin countries.
  • Worldwide genres (e.g. Jazz, Rock, Hip-hop, Electronic): include tracks from at least 4 different countries to showcase global diversity.
- Only include tracks you are confident exist on Spotify, Apple Music, or YouTube under the latin-script artist name. Do not invent track titles.
- "artists" MUST contain 6–8 real, globally recognizable artists for this genre, even if you are uncertain about specific track titles. Use the most commonly-used latin-script spelling of the artist name (e.g. "Sinn Sisamouth", not "ស៊ីន ស៊ីសាមុត"). The downstream system will resolve top tracks from these artists when track titles cannot be verified.
- For genres tied to non-Latin scripts (Khmer, Thai, Arabic, Mandarin, Hindi, Japanese, etc), prefer the latin-transliterated artist names that are most likely to exist on Spotify.
- artistCountry / artistCountryCode: ISO 3166-1 alpha-2 codes ("BR", "JP", "PT", "SN", "KH", "TH", etc).
- "suggestedGenres": 3 closely related genres for further exploration.`
: `First assess whether "${genre}" is a NICHE genre (practiced in 1–4 countries with a strong cultural identity, e.g. Malouf, Ma'luf, Gnawa, Byzantine Chant, Gamelan, Morna, Mbube, Jùjú, Tuvan throat singing, Sega, Taarab, Chaabi) or a BROAD genre (practiced worldwide or easily found in many countries as local variants, e.g. Jazz, Rock, Hip-hop, Folk, Classical, Pop, R&B, Electronic, Reggae, Metal).
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
      console.error(`[genre-spotlight] JSON.parse failed (${genre}/${country || "WORLDWIDE"}):`, parseErr.message, "\nRaw:", raw.slice(0, 200));
      return res.status(500).json({ error: "Failed to parse Claude response" });
    }
    // Worldwide mode returns artists as [{name, country, countryCode}]; country mode returns [name].
    const suggestedArtistsRaw = spotlight.artists || [];
    const suggestedArtists = suggestedArtistsRaw.map(a => (typeof a === "string" ? { name: a } : a));
    const artistCountryByName = new Map();
    if (worldwide) {
      for (const a of suggestedArtists) {
        if (a?.name && a?.country) {
          artistCountryByName.set(a.name.toLowerCase(), { country: a.country, countryCode: a.countryCode });
        }
      }
      // Also seed from Claude's tracks (they already carry artistCountry in worldwide mode)
      for (const t of spotlight.tracks || []) {
        if (t?.artist && t?.artistCountry && !artistCountryByName.has(t.artist.toLowerCase())) {
          artistCountryByName.set(t.artist.toLowerCase(), { country: t.artistCountry, countryCode: t.artistCountryCode });
        }
      }
    }
    const tagWorldwideTrack = (t) => {
      if (!worldwide || !t?.artist) return t;
      if (t.artistCountry && t.artistCountryCode) return t;
      const meta = artistCountryByName.get(t.artist.toLowerCase());
      return meta ? { ...t, artistCountry: t.artistCountry || meta.country, artistCountryCode: t.artistCountryCode || meta.countryCode } : t;
    };

    // Search streaming catalog for each track
    let tracks;

    if (service === "apple-music") {
      const appleToken = generateAppleMusicToken();
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!appleToken) return tagWorldwideTrack({ ...track, appleId: null });
          try {
            const q = `${track.title} ${track.artist}`;
            const r = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(q)}&types=songs&limit=5`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d = await r.json();
            const s = (d.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
            if (s) return tagWorldwideTrack({ ...track, appleId: s.id, previewUrl: s.attributes.previews?.[0]?.url || null });

            // Pass 2: title only, validate artist
            const r2 = await fetch(
              `https://api.music.apple.com/v1/catalog/us/search?term=${encodeURIComponent(track.title)}&types=songs&limit=5`,
              { headers: { Authorization: `Bearer ${appleToken}` } }
            );
            const d2 = await r2.json();
            const s2 = (d2.results?.songs?.data || []).find(s => artistNamesMatch(track.artist, s.attributes?.artistName || ""));
            return s2
              ? tagWorldwideTrack({ ...track, appleId: s2.id, previewUrl: s2.attributes.previews?.[0]?.url || null })
              : tagWorldwideTrack({ ...track, appleId: null, previewUrl: null });
          } catch { return tagWorldwideTrack({ ...track, appleId: null }); }
        })
      );
      tracks = tracksWithIds.filter(t => t.appleId).slice(0, 5);
    } else {
      const accessToken = await getClientAccessToken();

      // Phase 1: search for each Claude-suggested track directly
      const tracksWithIds = await Promise.all(
        (spotlight.tracks || []).map(async (track) => {
          if (!accessToken) return tagWorldwideTrack({ ...track, spotifyId: null });
          try {
            const q1 = `track:${track.title} artist:${track.artist}`;
            const r1 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(q1)}&type=track&limit=1&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d1 = await r1.json();
            const found1 = d1.tracks?.items?.[0];
            if (found1 && artistNamesMatch(track.artist, found1.artists?.[0]?.name || "")) {
              return tagWorldwideTrack({ ...track, spotifyId: found1.id, previewUrl: found1.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found1.id}` });
            }

            const r2 = await fetch(
              `https://api.spotify.com/v1/search?q=${encodeURIComponent(`${track.title} ${track.artist}`)}&type=track&limit=5&market=US`,
              { headers: { Authorization: "Bearer " + accessToken } }
            );
            const d2 = await r2.json();
            const found2 = (d2.tracks?.items || []).find(t => artistNamesMatch(track.artist, t.artists?.[0]?.name || ""));
            return found2
              ? tagWorldwideTrack({ ...track, spotifyId: found2.id, previewUrl: found2.preview_url || null, spotifyUrl: `https://open.spotify.com/track/${found2.id}` })
              : tagWorldwideTrack({ ...track, spotifyId: null });
          } catch { return tagWorldwideTrack({ ...track, spotifyId: null }); }
        })
      );
      tracks = tracksWithIds.filter(t => t.spotifyId);

      // Phase 2: if we have fewer than 4 tracks, search by artist name to fill gaps
      if (tracks.length < 4 && accessToken && suggestedArtists.length > 0) {
        console.log(`[genre-spotlight] only ${tracks.length} tracks found, trying artist-based fallback for ${suggestedArtists.length} artists`);
        const seenIds = new Set(tracks.map(t => t.spotifyId));

        const artistFallbackTracks = await Promise.all(
          suggestedArtists.map(async (artistObj) => {
            const artistName = artistObj.name;
            if (!artistName) return [];
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
              return (td.tracks || []).slice(0, 3).map(t => tagWorldwideTrack({
                title: t.name,
                artist: t.artists?.[0]?.name || artistName,
                spotifyId: t.id,
                previewUrl: t.preview_url || null,
                spotifyUrl: `https://open.spotify.com/track/${t.id}`,
                ...(worldwide && artistObj.country ? { artistCountry: artistObj.country, artistCountryCode: artistObj.countryCode } : {}),
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
            const lfRes = await lfFetch(
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
                  return tagWorldwideTrack({
                    title: found.name,
                    artist: found.artists?.[0]?.name || artist,
                    spotifyId: found.id,
                    previewUrl: found.preview_url || null,
                    spotifyUrl: `https://open.spotify.com/track/${found.id}`,
                  });
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
                  return tagWorldwideTrack({
                    title: found.name,
                    artist: found.artists?.[0]?.name || recArtist,
                    spotifyId: found.id,
                    previewUrl: found.preview_url || null,
                    spotifyUrl: `https://open.spotify.com/track/${found.id}`,
                  });
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
    if (!worldwide && spotlight.hasLocalScene === false && !spotlight.isNicheWorldGenre) tracks = [];
    if (worldwide && tracks.length === 0) {
      console.warn(`[genre-spotlight] WORLDWIDE empty result: genre="${genre}" — claude returned ${(spotlight.tracks || []).length} tracks, ${suggestedArtists.length} artists. First artist: ${suggestedArtists[0]?.name ?? "(none)"}`);
    }
    const isNicheWorldGenre = worldwide ? true : spotlight.isNicheWorldGenre === true;
    let gsResult = {
      genre,
      country,
      worldwide,
      explanation: spotlight.explanation,
      tracks,
      suggestedGenres: spotlight.suggestedGenres || [],
      hasLocalScene: worldwide ? true : spotlight.hasLocalScene !== false,
      isNicheWorldGenre,
    };

    // Supplement with related artist tracks from the recommendation page (runs on fresh responses too)
    gsResult = await supplementFromRelatedArtists(gsResult);

    await storeCache(gsCacheKey, "genre-spotlight", gsResult);
    console.log(`[genre-spotlight] Claude → ${genre} / ${country || "WORLDWIDE"} (${gsResult.tracks.length} tracks)`);
    res.json(stripPreviewUrls(gsResult));
  } catch (err) {
    console.error("Genre spotlight error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Take Me Deeper: suggest a niche subgenre ─────────────
app.post("/api/genre-deeper", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { genre, country: rawCountry, service = "spotify", visited = [] } = req.body;
  if (!genre) return res.status(400).json({ error: "Missing genre." });

  const country = (rawCountry || "").trim();
  const worldwide = !country;

  const cacheKey = makeCacheKey([
    "genre-deeper",
    genre,
    worldwide ? "WORLDWIDE" : country,
    ...(visited.length ? [visited.slice().sort().join(",")] : []),
  ]);
  const cached = await getCached(cacheKey);
  if (cached) {
    console.log(`[genre-deeper] cache hit → ${genre} / ${country || "WORLDWIDE"}`);
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
          content: worldwide
? `A listener just explored "${genre}" worldwide and wants to discover a closely related genre to dig into next.
${visited.length > 0 ? `\nGenres already visited (DO NOT suggest any of these or anything that is essentially the same under a different name): ${visited.map(g => `"${g}"`).join(", ")}\n` : ""}

Suggest ONE related genre to explore next. Good moves include:
- A more specific subgenre (e.g. Rock → Shoegaze, Soul → Southern Soul)
- A sibling genre that shares roots, era, or audience (e.g. Reggae → Dub, Highlife → Jùjú)
- A regional/diaspora variant of the same parent tradition (e.g. Rumba → Soukous)

Rules:
- The suggestion must be MEANINGFULLY DIFFERENT from "${genre}" — not the same genre under a different name.
- Must be a real genre with a genuine scene and real recordings — not a made-up label.
- Prefer genres with tracks available on Spotify, Apple Music, or YouTube.
- If "${genre}" is already extremely niche with no meaningful subgenres, suggest a related sibling or parent genre.

Return exactly:
{
  "genre": "specific genre name"
}`
: `A listener just explored "${genre}" from ${country} and wants to discover something new.
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
    if (worldwide) result.country = "";
    await storeCache(cacheKey, "genre-deeper", result);
    console.log(`[genre-deeper] Claude → "${result.genre}" / ${result.country || "WORLDWIDE"} (from ${genre} / ${country || "WORLDWIDE"})`);
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
    const stripped = tracks.map(({ previewUrl, ...rest }) => rest);
    artistTracksMemCache.set(cacheKey, { tracks: stripped, cachedAt: Date.now() });
    storeCache(cacheKey, "artist-tracks", { tracks: stripped }).catch(() => {});
    console.log(`  [artist-tracks] Apple Music → ${tracks.length} tracks for "${artistName}"`);
    return stripped;
  }

  // 4. ListenBrainz → Deezer/YouTube enrichment as final fallback
  const lbTracks = await fetchArtistTracksFromLB(artistName);
  const enriched = await enrichTracksWithYouTube(lbTracks, artistName);
  if (enriched.length > 0) {
    const strippedEnriched = enriched.map(({ previewUrl, ...rest }) => rest);
    artistTracksMemCache.set(cacheKey, { tracks: strippedEnriched, cachedAt: Date.now() });
    storeCache(cacheKey, "artist-tracks", { tracks: strippedEnriched }).catch(() => {});
  } else {
    storeCache(cacheKey, "artist-tracks", { tracks: [], flagged: true }).catch(() => {});
    console.log(`  [artist-tracks] no tracks found — flagged for deep enrich: "${artistName}"`);
  }
  return enriched.length > 0 ? strippedEnriched : [];
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

// Fetch Apple Music user's library artists
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

    const tracksStripped = tracks.map(({ previewUrl, ...rest }) => rest);
    artistTracksMemCache.set(cacheKey, { tracks: tracksStripped, cachedAt: Date.now() });
    if (tracksStripped.length > 0) {
      storeCache(cacheKey, "artist-tracks-apple", { tracks: tracksStripped }).catch(() => {});
      console.log(`  [artist-tracks-apple] cached ${tracksStripped.length} tracks → ${artistName}`);
    } else {
      storeCache(cacheKey, "artist-tracks-apple", { tracks: [], flagged: true }).catch(() => {});
      console.log(`  [artist-tracks-apple] no tracks found — flagged for deep enrich: "${artistName}"`);
    }
    res.json({ tracks: tracksStripped });
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

const SIMILAR_ARTISTS_SYSTEM_PROMPT = `You are a world music expert. Given a seed artist, return similar artists as JSON.

PRIMARY GOAL — sonic similarity, no fabrication:
- Every recommendation MUST plausibly sound similar to the seed artist in genre, instrumentation, vocal style, or era. Sonic fit is non-negotiable.
- NEVER invent artists. NEVER fabricate a country to satisfy a quota. If you truly know of fewer real sonic matches than asked for, return fewer.

SECONDARY GOAL — global discovery:
- This app is called "Musical Passport". Its purpose is helping listeners find genuine sonic peers from AROUND THE WORLD, not just the Anglosphere.
- Within the bar of real sonic similarity, ACTIVELY SEEK genuine non-Anglosphere peers. The Anglosphere here means US, UK, Canada, Australia, New Zealand, Ireland.
- Almost every broad genre has real, well-known non-Anglosphere practitioners:
  • Indie pop / bedroom pop → Aurora (NO), Sigrid (NO), Christine and the Queens (FR), Stella Donnelly (AU), Mitski (JP-US), Yuck (UK→JP), Hatchie (AU), No Vacation (PH-US), Beach Bunny (US), Cuco (MX-US), Yaeji (KR-US), Soccer Mommy (US), Mamiko Watanabe variants, Yumi Zouma (NZ), Faye Webster (US)... (mix anglo + non-anglo)
  • Hip-hop → Stormzy (UK), Aya Nakamura (FR/ML), Bad Bunny (PR), MC Solaar (FR), Skepta (UK/NG), Niska (FR), Tiakola (FR), Dadju (FR/CD), Beny Jr (ES), Jorja Smith (UK)
  • Electronic → Caribou (CA), Floating Points (UK), Nicola Cruz (EC), Dixon (DE), Kornel Kovacs (SE), Octo Octa (US), Peggy Gou (KR/DE), Honey Dijon (US)
  • Rock / shoegaze → Fishmans (JP), My Bloody Valentine (IE), Slowdive (UK), Lush (UK), Pinegrove (US), Black Country New Road (UK), DIIV (US)
  • R&B / soul → Sault (UK), Cleo Sol (UK), Joy Crookes (UK/BD), Greentea Peng (UK/NG), Kali Uchis (US/CO), Lous and the Yakuza (BE/CD), Téchnine (FR), Tems (NG)
- HARD RULE: If your initial 10 candidates contain 6+ Anglosphere artists, you have failed the secondary goal. Replace some of them with equally-strong real non-Anglosphere peers.
- Aim for at least 4 different countries across the result, and at least 3 of them outside the Anglosphere when real matches exist.

Other rules:
- "From" means country of origin: where a solo artist was born/raised or where a band formed. Do NOT use ancestry, a parent's country, residence, fanbase, label market, or cultural influence.
- Mix contemporary and classic artists.
- Avoid globally mainstream acts (no top-10 global chart artists).
- Only professional musicians — exclude actors, TV personalities, athletes, or celebrities who released music as a side project.
- Be precise about country of origin — do NOT guess.

Unknown or misspelled seed artists:
- If the seed name appears misspelled, ambiguous, or unfamiliar, INFER the most likely canonical artist from the spelling (e.g. "Novos Bianos" → "Novos Baianos") and proceed silently with that interpretation.
- NEVER refuse, NEVER explain, NEVER ask for clarification, NEVER narrate your reasoning. Always return the JSON schema. If you truly cannot infer anything, return { "artists": [] }.

Return ONLY valid JSON — no markdown, no backticks, no preamble. Schema:
{
  "artists": [
    {
      "name": "exact artist name in native script if applicable (e.g. 杏里, BTS, Fairuz)",
      "romanizedName": "romanized/English name ONLY if name uses non-Latin script — omit otherwise",
      "country": "full country name",
      "countryCode": "2-letter ISO code",
      "genre": "ONE primary genre, 1–3 words, no commas, no slashes (e.g. \\"indie pop\\", \\"shoegaze\\", \\"afrobeats\\")",
      "era": "EXACTLY ONE decade in the form NNNNs (e.g. \\"1980s\\", \\"2010s\\"). Never a range, never two decades. Pick the single decade the artist is most associated with."
    }
  ]
}`;

// Generic genre tokens that don't discriminate between artists. If these are the
// only words shared, it's not a real sonic match. Kept small and conservative.
const GENERIC_GENRE_TOKENS = new Set([
  'singer', 'songwriter', 'vocalists', 'vocalist', 'female', 'male',
  'music', 'band', 'group', 'solo', 'artist', 'world', 'international',
  'new', 'york', 'los', 'angeles', 'british', 'american', 'european',
]);

// Allowlist of common musical-genre tokens. The sonic-fit gate is only meaningful
// when the seed's Last.fm tags include at least one of these — otherwise the
// seed's tags are purely cultural/regional (e.g. "arabic, lebanese, lebanon" for
// Fairuz) and rejecting non-overlapping artists would over-filter perfectly good
// regional peers like Mohamed Mounir or Lena Chamamyan.
const MUSICAL_GENRE_TOKENS = new Set([
  'rock', 'pop', 'jazz', 'blues', 'folk', 'country', 'hip', 'hop', 'rap',
  'electronic', 'electro', 'dance', 'house', 'techno', 'classical', 'metal',
  'punk', 'indie', 'alternative', 'soul', 'funk', 'disco', 'reggae', 'ska',
  'bossa', 'samba', 'fado', 'flamenco', 'salsa', 'cumbia', 'tango', 'gospel',
  'ambient', 'trance', 'dubstep', 'grime', 'garage', 'experimental', 'avant',
  'progressive', 'psychedelic', 'shoegaze', 'hardcore', 'emo', 'rnb',
  'dub', 'jungle', 'breakbeat', 'idm', 'lounge', 'opera', 'orchestral',
  'chamber', 'baroque', 'romantic', 'choral', 'symphonic', 'piano', 'guitar',
  'instrumental', 'acoustic', 'singer', // singer is generic alone but signals "song-based"
  'mpb', 'tropicalia', 'rumba', 'bachata', 'merengue', 'mariachi', 'ranchera',
  'bluegrass', 'americana', 'gothic', 'industrial', 'synth', 'wave', 'darkwave',
  'krautrock', 'dreampop', 'trip', 'lofi', 'rai', 'qawwali', 'raga', 'klezmer',
  'mbalax', 'morna', 'highlife', 'afrobeat', 'soukous', 'kizomba',
]);

function genreTokens(s) {
  return (s || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // "Tropicália" → "Tropicalia"
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 3 && !GENERIC_GENRE_TOKENS.has(w));
}

// Take only the primary genre when Claude returns a comma/slash-joined list
// (e.g. "indie pop, bedroom pop, singer-songwriter" → "indie pop").
function normalizePrimaryGenre(g) {
  if (!g || typeof g !== 'string') return g;
  const first = g.split(/[,/|;]| and |&| · /i)[0].trim();
  return first || g;
}

// Coerce ranges like "2010s-2020s" / "2010s–2020s" / "2010s & 2020s"
// into a single decade (the latest — that's the artist's current era).
function normalizeEra(e) {
  if (!e || typeof e !== 'string') return e;
  const decades = e.match(/(?:19|20)\d0s/g);
  if (!decades || decades.length === 0) return e.trim();
  return decades[decades.length - 1];
}

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

// ── Similar-artists configuration + helpers ──────────────
// The endpoint returns this many artists on every response. Keep it stable so
// the frontend can lay out a consistent grid.
const SIMILAR_TARGET_COUNT = 10;
// Raw candidate pool cap — we keep ~2x the response size so reranks against
// future seeds (via the reverse-index) still have room to produce variety.
const SIMILAR_POOL_SIZE    = 24;

// Process-local anti-repetition LRU. Every artist we serve bumps a counter
// here; reranks apply a mild penalty so the same cluster (e.g. Kali Uchis,
// Tame Impala, Cuco) can't dominate every indie-pop result set in the same
// hour. Resets on server restart — intentionally lightweight.
const SIMILAR_LRU_TTL_MS = 60 * 60 * 1000; // 1h
const SIMILAR_LRU_MAX    = 400;
const recentlyServedSimilar = new Map(); // lowercase name → { lastAt, count }

function bumpRecentlyServed(name) {
  const key = (name || '').toLowerCase().trim();
  if (!key) return;
  const now = Date.now();
  const prev = recentlyServedSimilar.get(key);
  if (prev && now - prev.lastAt < SIMILAR_LRU_TTL_MS) {
    prev.lastAt = now;
    prev.count += 1;
    // Move to end so recent entries evict last
    recentlyServedSimilar.delete(key);
    recentlyServedSimilar.set(key, prev);
  } else {
    recentlyServedSimilar.set(key, { lastAt: now, count: 1 });
  }
  if (recentlyServedSimilar.size > SIMILAR_LRU_MAX) {
    const oldest = recentlyServedSimilar.keys().next().value;
    recentlyServedSimilar.delete(oldest);
  }
}

function recentPenalty(name) {
  const key = (name || '').toLowerCase().trim();
  const entry = recentlyServedSimilar.get(key);
  if (!entry) return 0;
  if (Date.now() - entry.lastAt > SIMILAR_LRU_TTL_MS) {
    recentlyServedSimilar.delete(key);
    return 0;
  }
  // Linear -0.06 per repeat, capped at -0.24 (≈ one tier in rerank score,
  // never fully kills a strong match).
  return Math.min(0.24, entry.count * 0.06);
}

// Per-artist top tags from Last.fm — the core sonic-fit signal. Used to
// objectively score how close a candidate's genre DNA is to the seed's.
async function lastfmArtistTopTags(artistName) {
  const lastfmKey = process.env.LASTFM_API_KEY;
  if (!lastfmKey || !artistName) return [];
  try {
    const r = await lfFetch(
      `https://ws.audioscrobbler.com/2.0/?method=artist.getTopTags&artist=${encodeURIComponent(artistName)}&autocorrect=1&api_key=${lastfmKey}&format=json`
    );
    if (!r || r.ok === false || typeof r.json !== 'function') return [];
    const d = await r.json();
    if (d.error) return [];
    return (d.toptags?.tag || [])
      .map(t => t.name)
      .filter(t => typeof t === 'string' && t.length > 0 && t.length < 40)
      .slice(0, 8);
  } catch { return []; }
}

async function lastfmTopTagsBatch(artistNames) {
  const unique = [...new Set((artistNames || []).filter(Boolean))];
  const entries = await Promise.all(
    unique.map(async (name) => [name, await lastfmArtistTopTags(name)])
  );
  return new Map(entries);
}

// Last.fm tag.getTopArtists — fallback candidate source used when similar +
// related give too few country-verifiable options (obscure indie seeds, tiny
// regional scenes). Does not guarantee global coverage — that's fine, we
// apply MB country verification and drop unresolvable entries afterward.
async function lastfmTagTopArtists(tag, limit = 20) {
  const lastfmKey = process.env.LASTFM_API_KEY;
  if (!lastfmKey || !tag) return [];
  try {
    const r = await lfFetch(
      `https://ws.audioscrobbler.com/2.0/?method=tag.getTopArtists&tag=${encodeURIComponent(tag)}&limit=${limit}&api_key=${lastfmKey}&format=json`
    );
    if (!r || r.ok === false || typeof r.json !== 'function') return [];
    const d = await r.json();
    if (d.error) return [];
    return (d.topartists?.artist || [])
      .map(a => a?.name)
      .filter(Boolean);
  } catch { return []; }
}

// Rerank a pool against the current seed's tag profile. This is called from
// every served path (direct cache hit, reverse-index hit, fresh generation)
// so identical source pools still produce distinct orderings per seed — the
// key fix for the Men I Trust / Moon Panda / Shintaro Sakamoto collapse.
//
// The bonus is Jaccard-like over non-generic genre tokens. Candidates with
// empty tags (older cache rows) fall through unaffected and rank purely by
// their stored base match score.
function rerankSimilarPool(pool, { seedTagSet }) {
  const hasSeedSignal = seedTagSet && seedTagSet.size > 0;
  const rescored = pool.map(a => {
    const base = typeof a.match === 'number' ? a.match : 0.35;
    const tags = Array.isArray(a.tags) ? a.tags : [];
    let overlap = 0;
    if (hasSeedSignal && tags.length > 0) {
      const candTokens = new Set(tags.flatMap(genreTokens));
      if (candTokens.size > 0) {
        let shared = 0;
        for (const t of candTokens) if (seedTagSet.has(t)) shared++;
        overlap = shared / Math.max(1, candTokens.size);
      }
    }
    const bonus = overlap * 0.35;
    const penalty = recentPenalty(a.name);
    return { ...a, rerankScore: base + bonus - penalty };
  });
  return rescored.sort((a, b) => {
    const d = (b.rerankScore || 0) - (a.rerankScore || 0);
    if (d !== 0) return d;
    return (a.name || '').localeCompare(b.name || '');
  });
}

// Override clearly-wrong Claude country codes when the genre string itself
// explicitly names a country/region. Catches the Nasty C=JP / Keith Ape=US
// pattern: Claude writes the correct genre ("South African Hip-Hop/Trap")
// but picks a random country. Uses only the existing genre hint table —
// when the genre is region-neutral (e.g. "folk rock"), we leave Claude's
// country alone so MB false positives (Nick Drake GB→CA) can't happen.
function applyGenreCountryHints(pool) {
  return pool.map(a => {
    if (a.mbVerified === true) return a;        // trust real MB verification
    if (a.mbVerified === 'tag') return a;       // tag nationality is stronger than genre guess
    if (!a.genre) return a;
    const hintCode = countryHintFromGenre(a.genre);
    if (!hintCode || !ISO_TO_COUNTRY[hintCode]) return a;
    if (a.countryCode === hintCode) return a;   // already matches
    if (a.countryCode) {
      console.log(`[genre-hint] ${a.name}: ${a.countryCode} → ${hintCode} (genre: ${a.genre})`);
    }
    return {
      ...a,
      countryCode: hintCode,
      country: ISO_TO_COUNTRY[hintCode],
      mbVerified: 'genre',
    };
  });
}

// Last.fm user-submitted tag → ISO-2 nationality map. Crowd-verified per
// artist, so far more reliable than Claude for famous Anglosphere acts that
// Claude sometimes "diversifies" with fabricated origins (ZAYN=PK because
// his father is Pakistani, etc.). Checked BEFORE the genre-hint pass.
const NATIONALITY_TAG_TO_CODE = {
  british: 'GB', english: 'GB', welsh: 'GB', scottish: 'GB',
  uk: 'GB', britain: 'GB', england: 'GB', scotland: 'GB', wales: 'GB',
  american: 'US', usa: 'US', 'united states': 'US', 'u s a': 'US',
  canadian: 'CA', canada: 'CA',
  australian: 'AU', australia: 'AU', aussie: 'AU',
  irish: 'IE', ireland: 'IE',
  'new zealand': 'NZ', kiwi: 'NZ', nz: 'NZ',
  french: 'FR', france: 'FR',
  german: 'DE', germany: 'DE', deutsch: 'DE', deutsche: 'DE',
  italian: 'IT', italy: 'IT', italia: 'IT',
  spanish: 'ES', spain: 'ES',
  portuguese: 'PT', portugal: 'PT',
  swedish: 'SE', sweden: 'SE', sverige: 'SE',
  norwegian: 'NO', norway: 'NO', norge: 'NO',
  danish: 'DK', denmark: 'DK',
  finnish: 'FI', finland: 'FI', suomi: 'FI',
  icelandic: 'IS', iceland: 'IS',
  dutch: 'NL', netherlands: 'NL', holland: 'NL',
  belgian: 'BE', belgium: 'BE',
  swiss: 'CH', switzerland: 'CH',
  austrian: 'AT', austria: 'AT',
  polish: 'PL', poland: 'PL',
  russian: 'RU', russia: 'RU',
  ukrainian: 'UA', ukraine: 'UA',
  greek: 'GR', greece: 'GR',
  turkish: 'TR', turkey: 'TR',
  japanese: 'JP', japan: 'JP',
  korean: 'KR', korea: 'KR', 'south korea': 'KR',
  chinese: 'CN', china: 'CN',
  indian: 'IN', india: 'IN',
  pakistani: 'PK', pakistan: 'PK',
  indonesian: 'ID', indonesia: 'ID',
  thai: 'TH', thailand: 'TH',
  vietnamese: 'VN', vietnam: 'VN',
  filipino: 'PH', philippines: 'PH',
  brazilian: 'BR', brazil: 'BR', brasil: 'BR',
  mexican: 'MX', mexico: 'MX',
  argentine: 'AR', argentina: 'AR', argentinian: 'AR',
  colombian: 'CO', colombia: 'CO',
  chilean: 'CL', chile: 'CL',
  peruvian: 'PE', peru: 'PE',
  cuban: 'CU', cuba: 'CU',
  'puerto rican': 'PR', 'puerto rico': 'PR',
  jamaican: 'JM', jamaica: 'JM',
  'south african': 'ZA', 'south africa': 'ZA',
  nigerian: 'NG', nigeria: 'NG',
  ghanaian: 'GH', ghana: 'GH',
  ethiopian: 'ET', ethiopia: 'ET',
  kenyan: 'KE', kenya: 'KE',
  egyptian: 'EG', egypt: 'EG',
  moroccan: 'MA', morocco: 'MA',
  algerian: 'DZ', algeria: 'DZ',
  lebanese: 'LB', lebanon: 'LB',
  israeli: 'IL', israel: 'IL',
  iranian: 'IR', iran: 'IR', persian: 'IR',
};

// Pre-pass: override Claude country codes when the candidate's Last.fm tags
// explicitly name a nationality AND the existing country has no tag support
// of its own. Catches Claude's "ancestry, not origin" failure mode (ZAYN=PK
// because his father is Pakistani, RAYE=ZA, Wallows=NZ etc.) without
// breaking artists whose tags legitimately mention multiple nationalities
// (Tinashe has both 'british' and 'american' tags — Claude said US, we
// keep US because 'american' is in the list).
//
// Rule:
//   1. Collect the set of ISO codes implied by all nationality tags.
//   2. If the existing countryCode is in that set, KEEP it (corroborated).
//   3. Otherwise, override to the FIRST matched code (tag-order consensus).
function applyTagNationalityHints(pool) {
  return pool.map(a => {
    if (a.mbVerified === true) return a;
    const tags = Array.isArray(a.tags) ? a.tags : [];
    if (tags.length === 0) return a;
    // Scan all tags, collect every nationality code mentioned and remember
    // the first one (which is the most-voted consensus per Last.fm ordering).
    const tagCodes = new Set();
    let firstCode = null;
    for (const tag of tags) {
      const norm = (tag || '').toLowerCase().trim();
      if (!norm) continue;
      const code = NATIONALITY_TAG_TO_CODE[norm];
      if (code && ISO_TO_COUNTRY[code]) {
        tagCodes.add(code);
        if (!firstCode) firstCode = code;
      }
    }
    if (!firstCode) return a;
    // If the existing country is corroborated by ANY nationality tag, keep it.
    // This prevents the Tinashe-style false positive where tags include both
    // 'british' (position 4) and 'american' (position 7) — Claude correctly
    // says US and 'american' supports it, so we don't flip to GB.
    if (a.countryCode && tagCodes.has(a.countryCode)) return a;
    // No corroboration — use the first (most-voted) nationality tag.
    if (a.countryCode) {
      console.log(`[tag-hint] ${a.name}: ${a.countryCode} → ${firstCode} (tag match, no corroboration)`);
    }
    return {
      ...a,
      countryCode: firstCode,
      country: ISO_TO_COUNTRY[firstCode],
      mbVerified: 'tag',
    };
  });
}

// pickDiverse's strict country-uniqueness + region round-robin can return
// fewer than n items when the pool's country coverage is thin. This wrapper
// loosens the constraint progressively rather than ever returning short.
function pickDiverseProgressive(pool, n) {
  const strict = pickDiverse(pool, n);
  if (strict.length >= n || pool.length === 0) return strict.slice(0, n);

  const chosenKey = new Set(strict.map(a => (a.name || '').toLowerCase().trim()));
  const perCountry = {};
  for (const a of strict) {
    const k = a.country || a.countryCode || '';
    perCountry[k] = (perCountry[k] || 0) + 1;
  }
  // rerankScore / match-sorted leftovers. pickDiverse already sorted, and
  // upstream rerankSimilarPool also sorted, so walking in pool order is fine.
  const relaxed = [];
  for (const a of pool) {
    if (strict.length + relaxed.length >= n) break;
    const low = (a.name || '').toLowerCase().trim();
    if (chosenKey.has(low)) continue;
    const ck = a.country || a.countryCode || '';
    if ((perCountry[ck] || 0) >= 2) continue;
    perCountry[ck] = (perCountry[ck] || 0) + 1;
    chosenKey.add(low);
    relaxed.push(a);
  }
  if (strict.length + relaxed.length >= n) return [...strict, ...relaxed];

  // Last resort: fill from any remaining pool entry in order.
  const finalFill = [];
  for (const a of pool) {
    if (strict.length + relaxed.length + finalFill.length >= n) break;
    const low = (a.name || '').toLowerCase().trim();
    if (chosenKey.has(low)) continue;
    chosenKey.add(low);
    finalFill.push(a);
  }
  return [...strict, ...relaxed, ...finalFill];
}

app.post("/api/similar-artists", async (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_API_KEY is not set." });

  const { artistName } = req.body;
  if (!artistName) return res.status(400).json({ error: "Missing artistName." });

  const simCacheKey = makeCacheKey(["similar", artistName]);
  const baseNorm    = artistName.toLowerCase().trim();
  const lastfmKey   = process.env.LASTFM_API_KEY;

  // Remove the base artist (and any explicitly-named aliases, e.g. the source
  // pool base when serving from the reverse-index) from a candidate pool.
  function filterBase(pool, ...extraBases) {
    const bases = new Set([baseNorm, ...extraBases.filter(Boolean).map(s => s.toLowerCase().trim())]);
    return pool.filter(a => !bases.has((a.name || '').toLowerCase().trim()));
  }

  async function ensureImages(artists) {
    const need = artists.filter(a => !a.imageUrl);
    if (need.length === 0) return artists;
    const urls = await Promise.all(need.map(a =>
      fetchArtistImageUrl(a.name, { genre: a.genre, skipSpotify: true }).catch(() => null)
    ));
    const urlByName = new Map(need.map((a, i) => [a.name, urls[i] || null]));
    return artists.map(a => a.imageUrl ? a : { ...a, imageUrl: urlByName.get(a.name) || null });
  }

  async function parseLfResponse(r) {
    if (!r || r.ok === false || typeof r.json !== 'function') return null;
    try { return await r.json(); }
    catch { return null; }
  }

  // Synchronously fetch the current seed's Last.fm info so cache-hit and
  // reverse-index paths have fresh tags to rerank against (not the source's).
  async function fetchSeedInfo(name) {
    if (!lastfmKey) return { foundName: name, tags: [] };
    try {
      const r = await lfFetch(
        `https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(name)}&autocorrect=1&api_key=${lastfmKey}&format=json`
      );
      const d = await parseLfResponse(r);
      if (!d || d.error || !d.artist) return { foundName: name, tags: [] };
      return {
        foundName: d.artist.name || name,
        tags: (d.artist.tags?.tag || [])
          .map(t => t.name)
          .filter(t => typeof t === 'string' && t.length < 40)
          .slice(0, 8),
      };
    } catch { return { foundName: name, tags: [] }; }
  }

  // Shared terminal step: rerank → filter → pick → image → LRU-bump.
  async function serveFromPool(pool, seedName, seedTags, extraBase) {
    const seedTagSet = new Set((seedTags || []).flatMap(genreTokens));
    const reranked = rerankSimilarPool(pool, { seedTagSet });
    const filtered = filterBase(reranked, seedName, extraBase);
    const selected = pickDiverseProgressive(filtered, SIMILAR_TARGET_COUNT);
    const withImages = await ensureImages(selected);
    withImages.forEach(a => bumpRecentlyServed(a.name));
    // Normalize legacy/cached genre & era so the client always gets a single
    // primary genre and a single decade, even if Claude or older code paths
    // produced "indie pop, bedroom pop" or "2010s–2020s".
    return withImages.map(a => ({ ...a, genre: normalizePrimaryGenre(a.genre), era: normalizeEra(a.era) }));
  }

  // ── 1. Direct cache hit — rerank on every serve ───────────────────────
  const simCached = await getCached(simCacheKey);
  if (simCached && simCached.artist_pool && simCached.artist_pool.length >= 4) {
    console.log(`[similar-artists] cache hit → ${artistName}`);
    const baseArtist = simCached.result?.baseArtist || artistName;
    const cachedSeedTags = simCached.result?.seedTags || [];
    const served = await serveFromPool(simCached.artist_pool, baseArtist, cachedSeedTags);
    res.json({ baseArtist, artists: served });

    // SWR: heal missing countries + images. Uses the same genre-hint
    // pre-pass as fresh generation so legacy Claude-country errors caught
    // by the hint table (Nasty C=JP, Keith Ape=US) get corrected, while
    // ambiguous names (Nick Drake) keep whatever the pool already holds.
    const needsVerify = simCached.artist_pool.some(a => a.mbVerified !== true);
    const needsImages = simCached.artist_pool.some(a => !a.imageUrl);
    if ((needsVerify || needsImages) && !similarSwrInFlight.has(simCacheKey)) {
      similarSwrInFlight.add(simCacheKey);
      setImmediate(async () => {
        try {
          let pool = simCached.artist_pool;
          if (needsVerify) {
            pool = applyTagNationalityHints(pool);
            pool = applyGenreCountryHints(pool);
            pool = await verifyPoolCountries(pool);
          }
          if (needsImages) pool = await ensureImages(pool);
          await storeCache(simCacheKey, "similar-artists", {
            ...simCached.result,
            baseArtist,
            countryVerified: true,
          }, pool);
        } catch (err) {
          console.warn(`[similar-artists] SWR heal failed: ${err?.message || err}`);
        }
        similarSwrInFlight.delete(simCacheKey);
      });
    }
    return;
  }

  // ── 2. Reverse-index hit — rerank against THIS seed's tags ────────────
  // This is the key fix for the Men I Trust / Moon Panda / Shintaro Sakamoto
  // collapse: identical source pools now produce distinct orderings because
  // the rerank uses the requested seed's own tag profile + LRU penalty.
  const reverseEntry = await getCached(makeCacheKey(["similar-of", artistName]));
  if (reverseEntry?.result?.poolKey) {
    const sourcePool = await getCached(reverseEntry.result.poolKey);
    if (sourcePool?.artist_pool?.length >= 4) {
      console.log(`[similar-artists] reverse-index hit → ${artistName} from ${reverseEntry.result.poolKey}`);
      const { foundName, tags: seedTags } = await fetchSeedInfo(artistName);
      const sourceBaseArtist = sourcePool.result?.baseArtist;
      const served = await serveFromPool(sourcePool.artist_pool, foundName, seedTags, sourceBaseArtist);
      res.json({ baseArtist: foundName, artists: served });

      if (!similarSwrInFlight.has(simCacheKey)) {
        similarSwrInFlight.add(simCacheKey);
        setImmediate(async () => {
          try {
            // Persist under this seed's key, with the source base explicitly
            // removed so it can never reappear in this seed's response.
            const filteredForCache = filterBase(sourcePool.artist_pool, foundName, sourceBaseArtist);
            await storeCache(simCacheKey, "similar-artists", {
              baseArtist: foundName,
              seedTags,
              countryVerified: sourcePool.result?.countryVerified || false,
            }, filteredForCache);
            await storeSimilarIndex(filteredForCache, simCacheKey);
          } catch (err) {
            console.warn(`[similar-artists] reverse-index persist failed: ${err?.message || err}`);
          }
          similarSwrInFlight.delete(simCacheKey);
        });
      }
      return;
    }
  }

  // ── 3. Fresh generation — in-flight dedup ─────────────────────────────
  const _simKey = baseNorm;
  if (similarInFlight.has(_simKey)) {
    console.log(`[similar-artists] coalescing → ${artistName}`);
    try { return res.json(await similarInFlight.get(_simKey)); }
    catch (err) { return res.status(500).json({ error: err.message }); }
  }
  let _simResolve = null;
  let _simReject  = null;
  similarInFlight.set(_simKey, new Promise((ok, fail) => { _simResolve = ok; _simReject = fail; }));

  try {
    let foundName = artistName;
    let seedTags = [];
    let lastfmSimilar = [];
    let deezerSimilar = [];

    // Parallel: Last.fm info+similar + Deezer related.
    await Promise.all([
      (async () => {
        if (!lastfmKey) return;
        try {
          const [infoRes, simRes] = await Promise.all([
            lfFetch(`https://ws.audioscrobbler.com/2.0/?method=artist.getInfo&artist=${encodeURIComponent(artistName)}&autocorrect=1&api_key=${lastfmKey}&format=json`),
            lfFetch(`https://ws.audioscrobbler.com/2.0/?method=artist.getSimilar&artist=${encodeURIComponent(artistName)}&limit=30&autocorrect=1&api_key=${lastfmKey}&format=json`),
          ]);
          const [infoData, simData] = await Promise.all([parseLfResponse(infoRes), parseLfResponse(simRes)]);
          if (infoData && !infoData.error && infoData.artist?.name) {
            foundName = infoData.artist.name;
            seedTags = (infoData.artist.tags?.tag || [])
              .map(t => t.name)
              .filter(t => typeof t === 'string' && t.length < 40)
              .slice(0, 8);
            console.log(`[similar-artists] Last.fm info → ${foundName}, tags: ${seedTags.join(", ")}`);
          } else {
            console.log(`[similar-artists] Last.fm info unavailable for "${artistName}"`);
          }
          if (simData && !simData.error) {
            lastfmSimilar = (simData.similarartists?.artist || [])
              .map(a => ({ name: a.name, match: parseFloat(a.match) }))
              .filter(a => a.match > 0.1)
              .slice(0, 30);
            console.log(`[similar-artists] Last.fm similar → ${lastfmSimilar.length} artists for ${foundName}`);
          }
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

    const seedTagSet = new Set(seedTags.flatMap(genreTokens));
    // Pre-compute whether the seed has any actual musical-genre tokens — used
    // to enable/disable the sonic-fit filter for Claude candidates below.
    let seedHasMusicalSignal = false;
    for (const t of seedTagSet) {
      if (MUSICAL_GENRE_TOKENS.has(t)) { seedHasMusicalSignal = true; break; }
    }

    // Merge Last.fm + Deezer candidates by normalized name. Last.fm carries
    // real similarity scores; Deezer entries get synthetic scores slotted
    // between Last.fm's top and mid tiers.
    const combinedByKey = new Map();
    for (const a of lastfmSimilar) {
      combinedByKey.set(a.name.toLowerCase().trim(), { name: a.name, match: a.match, source: 'lastfm' });
    }
    deezerSimilar.forEach((name, i) => {
      const key = name.toLowerCase().trim();
      if (!key || combinedByKey.has(key)) return;
      const synthMatch = 0.60 - (Math.min(i, 20) / 20) * 0.20;
      combinedByKey.set(key, { name, match: synthMatch, source: 'deezer' });
    });

    // Rate-limit hygiene: Last.fm's lfQueue enforces 300ms between calls
    // (3.3/s — inside Last.fm's 5/s community guideline). Every tag fetch
    // adds ~300ms to the total latency, so we deliberately skip tag-fetching
    // directPool candidates: Last.fm's artist.getSimilar already returns a
    // real sonic-similarity score for them, and Deezer's synth scores slot
    // behind at 0.40-0.60. rerankSimilarPool still applies the LRU penalty
    // to these candidates even without tags. Tag-fetching is reserved for
    // sources that need it most: Claude (hallucination + sonic-fit gate) and
    // Stage 3 tag.getTopArtists (which have no similarity score at all).
    const allCandidates = [...combinedByKey.values()].sort((a, b) => (b.match || 0) - (a.match || 0));
    const allNamesForMb = allCandidates.map(c => c.name);
    console.log(`[similar-artists] merging ${allCandidates.length} candidates from Last.fm + Deezer`);
    const mbBatch = await getMbArtistCachedBatch(allNamesForMb);

    // Build the working pool from candidates with resolvable countries.
    // Candidates without an MB country are dropped rather than shipped with
    // a Claude-only country guess.
    let pool = [];
    for (const c of allCandidates) {
      const code = mbBatch.get(c.name.toLowerCase().trim());
      if (!code || !REGION_BY_CODE[code]) continue;
      pool.push({
        name: c.name,
        match: c.match,
        source: c.source,
        tags: [],               // directPool skips tag-fetching for latency
        genre: seedTags[0] || '',
        era: null,
        countryCode: code,
        country: ISO_TO_COUNTRY[code] || code,
        mbVerified: true,
      });
    }
    pool = filterBase(pool, foundName);
    console.log(`[similar-artists] directPool: ${pool.length} MB-verified of ${allCandidates.length}`);

    // ── Stage 2: Claude gap-fill ───────────────────────────────────────
    // Runs when the pool is either short of the target OR insufficiently
    // diverse. Last.fm + Deezer skew heavily Anglosphere by listener volume;
    // for an app called "Musical Passport" we want at least a few genuine
    // non-Anglosphere peers in every result set.
    const ANGLOSPHERE_CODES = new Set(["US", "GB", "CA", "AU", "NZ", "IE"]);
    const nonAngloCountriesInPool = new Set(
      pool.filter(a => a.countryCode && !ANGLOSPHERE_CODES.has(a.countryCode)).map(a => a.countryCode)
    );
    const needsDiversityFill = nonAngloCountriesInPool.size < 3;
    if (needsDiversityFill && pool.length >= SIMILAR_TARGET_COUNT) {
      console.log(`[similar-artists] diversity-fill triggered: pool has ${pool.length} but only ${nonAngloCountriesInPool.size} non-Anglo countries`);
    }
    if (pool.length < SIMILAR_TARGET_COUNT || needsDiversityFill) {
      // Ask for enough to buffer past filtering + dedup + sonic-fit rejection.
      const needCount = Math.max((SIMILAR_TARGET_COUNT + 4) - pool.length, 5);
      const alreadyLines = pool.length > 0
        ? `\n\nAlready-sourced artists — DO NOT repeat these names or their countries:\n${pool.slice(0, 14).map(a => `- ${a.name} (${a.country})`).join('\n')}`
        : '';
      const tagLines = seedTags.length > 0 ? `- Known genres/tags: ${seedTags.join(', ')}\n` : '';
      const lfLines = lastfmSimilar.length > 0
        ? `\nLast.fm similar artists (sonic reference, already ranked):\n${lastfmSimilar.slice(0, 15).map(a => `- ${a.name} (${(a.match * 100).toFixed(0)}%)`).join('\n')}`
        : '';

      const diversityNote = needsDiversityFill && pool.length >= SIMILAR_TARGET_COUNT
        ? `\nThe already-sourced pool above is heavy on Anglosphere artists — focus this fill ENTIRELY on real non-Anglosphere sonic peers (continental Europe, Latin America, Asia, Africa, Middle East, Oceania).`
        : '';
      const prompt = `Find up to ${needCount} artists who sound genuinely similar to ${foundName}.

Their profile:
${tagLines}${lfLines}${alreadyLines}${diversityNote}

Rules:
- Sonic fit is the bar. Never fabricate.
- Within that bar, ACTIVELY include real non-Anglosphere peers (Anglosphere = US/UK/Canada/Australia/NZ/Ireland). For broad genres like indie pop, hip-hop, electronic, R&B, rock — well-known non-Anglo peers exist; find them. Examples: Aurora (NO) and Christine and the Queens (FR) for indie pop; Aya Nakamura (FR) for R&B; Peggy Gou (KR/DE) for house; Bad Bunny (PR) for reggaeton/Latin trap; Fishmans (JP) for shoegaze; Stormzy (UK) for grime.
- Of the ${needCount} you return, NO MORE THAN HALF may be from the Anglosphere. If you cannot find enough real non-Anglo peers to satisfy that, return fewer artists — never invent filler.
- NEVER fabricate an artist's country. Report each artist's REAL country where they were born/raised or where the band formed — ancestry, parents' origin, residence, or fanbase do NOT count.
- Spread across at least 3–4 different countries when real options exist.
- Return fewer than ${needCount} if you don't know enough real sonic matches — NEVER invent filler.`;

      try {
        const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-api-key": apiKey,
            "anthropic-version": "2023-06-01",
          },
          body: JSON.stringify({
            model: "claude-haiku-4-5-20251001",
            max_tokens: 1200,
            system: SIMILAR_ARTISTS_SYSTEM_PROMPT,
            messages: [{ role: "user", content: prompt }],
          }),
        });
        const claudeData = await claudeRes.json();
        let claudeArtists = [];
        if (!claudeData.error) {
          try {
            const raw = (claudeData.content?.[0]?.text || "").replace(/```json|```/g, "").trim();
            const parsed = JSON.parse(raw);
            if (Array.isArray(parsed.artists)) claudeArtists = parsed.artists;
          } catch (parseErr) {
            console.warn(`[similar-artists] Claude JSON parse failed: ${parseErr.message}`);
          }
        } else {
          console.warn(`[similar-artists] Claude error: ${claudeData.error.message}`);
        }

        // Batch-fetch Last.fm top tags for every Claude candidate. Absent
        // response == probable hallucination (Last.fm has never heard of them).
        const claudeNames = claudeArtists.map(a => a?.name).filter(Boolean);
        const claudeTagsMap = claudeNames.length > 0
          ? await lastfmTopTagsBatch(claudeNames)
          : new Map();

        const existingLower = new Set(pool.map(p => (p.name || '').toLowerCase().trim()));
        let acceptedCount = 0;
        for (const a of claudeArtists) {
          if (!a?.name) continue;
          const low = a.name.toLowerCase().trim();
          if (low === baseNorm || existingLower.has(low)) continue;
          const tags = claudeTagsMap.get(a.name) || [];
          if (tags.length === 0 && lastfmKey) {
            console.log(`[similar-artists] dropping probable hallucination: ${a.name}`);
            continue;
          }
          // Sonic-fit check: soft penalty, not a hard drop. Seeds with quirky
          // or historical Last.fm tags (e.g. Sia's trip-hop/chillout from her
          // 2000s era) would otherwise kneecap great pop-era matches like
          // Robyn or Charli XCX for zero-token-overlap. Keeping them in the
          // pool with a lower match score lets rerank sort them below better
          // fits but still ship them when the pool is thin.
          let baseMatch = 0.38;
          if (seedHasMusicalSignal && tags.length > 0) {
            const candTokens = new Set(tags.flatMap(genreTokens));
            let hasOverlap = false;
            for (const t of candTokens) {
              if (seedTagSet.has(t)) { hasOverlap = true; break; }
            }
            if (!hasOverlap) {
              baseMatch -= 0.15;
              console.log(`[similar-artists] sonic-penalty ${a.name} (${tags.slice(0,3).join(', ')}) → match ${baseMatch.toFixed(2)}`);
            }
          }
          pool.push({
            name: a.name,
            romanizedName: a.romanizedName,
            country: a.country,
            countryCode: a.countryCode,
            genre: normalizePrimaryGenre(a.genre || tags[0] || seedTags[0] || ''),
            era: normalizeEra(a.era),
            match: baseMatch,
            source: 'claude',
            tags,
          });
          existingLower.add(low);
          acceptedCount++;
        }
        console.log(`[similar-artists] Claude accepted: ${acceptedCount}/${claudeArtists.length}`);
      } catch (e) {
        console.warn(`[similar-artists] Claude fetch failed: ${e.message}`);
      }
    }

    // ── Stage 3: Last.fm tag.getTopArtists fallback ────────────────────
    // Only triggered when the combined pool is still under the target size
    // (obscure Western indie, tiny regional scenes, misspelled inputs…).
    if (pool.length < SIMILAR_TARGET_COUNT && seedTags.length > 0) {
      const topTag = seedTags.find(t => MUSICAL_GENRE_TOKENS.has((t || '').toLowerCase())) || seedTags[0];
      if (topTag) {
        try {
          const tagArtists = await lastfmTagTopArtists(topTag, 30);
          const existing = new Set(pool.map(p => (p.name || '').toLowerCase().trim()));
          existing.add(baseNorm);
          const fresh = tagArtists.filter(n => !existing.has(n.toLowerCase().trim())).slice(0, 15);
          if (fresh.length > 0) {
            const [newMb, newTags] = await Promise.all([
              getMbArtistCachedBatch(fresh),
              lastfmTopTagsBatch(fresh),
            ]);
            let added = 0;
            for (const name of fresh) {
              if (pool.length >= SIMILAR_POOL_SIZE) break;
              const code = newMb.get(name.toLowerCase().trim());
              if (!code || !REGION_BY_CODE[code]) continue;
              pool.push({
                name,
                country: ISO_TO_COUNTRY[code] || code,
                countryCode: code,
                genre: (newTags.get(name) || [])[0] || seedTags[0] || '',
                era: null,
                match: 0.32,
                source: 'lastfm-tag',
                tags: newTags.get(name) || [],
                mbVerified: true,
              });
              added++;
            }
            if (added > 0) console.log(`[similar-artists] tag.getTopArtists("${topTag}") added ${added}`);
          }
        } catch (e) {
          console.warn(`[similar-artists] tag.getTopArtists failed: ${e.message}`);
        }
      }
    }

    // ── Country verification ──────────────────────────────────────────
    // Three-step so we don't inherit the old "MB is authoritative" failure
    // mode (e.g. MB has a single exact match for "Nick Drake" in Canada that
    // outranks the famous British one).
    //   Step 1: tag-nationality hints — overrides Claude when a candidate's
    //           Last.fm tags explicitly name a nationality ("british",
    //           "american", etc.). Catches Claude's "ancestry not origin"
    //           failure like ZAYN=PK, RAYE=ZA.
    //   Step 2: genre-country hints — catches Nasty C=JP (genre says
    //           "South African Hip-Hop") without touching ambiguous names.
    //   Step 3: verifyPoolCountries — fills MISSING codes from MB but
    //           leaves Claude's stated countries alone for artists with
    //           no nationality tag and a region-neutral genre.
    const tagHinted = applyTagNationalityHints(pool);
    const genreHinted = applyGenreCountryHints(tagHinted);
    const fullyVerified = await verifyPoolCountries(genreHinted);
    let finalPool = fullyVerified.filter(a => a.countryCode && ISO_TO_COUNTRY[a.countryCode]);
    finalPool = filterBase(finalPool, foundName);
    finalPool = rerankSimilarPool(finalPool, { seedTagSet });

    // Response selection + image fetch (only for the 10 we actually serve).
    const selected = pickDiverseProgressive(finalPool, SIMILAR_TARGET_COUNT);
    const responseImageUrls = await Promise.all(selected.map(a =>
      fetchArtistImageUrl(a.name, { genre: a.genre, skipSpotify: true }).catch(() => null)
    ));
    const responseWithImages = selected.map((a, i) => ({
      ...a,
      genre: normalizePrimaryGenre(a.genre),
      era: normalizeEra(a.era),
      imageUrl: responseImageUrls[i] || a.imageUrl || null,
    }));
    responseWithImages.forEach(a => bumpRecentlyServed(a.name));

    // Cache the served rows (with images) followed by buffer members.
    const servedNameSet = new Set(responseWithImages.map(a => a.name));
    const bufferMembers = finalPool.filter(a => !servedNameSet.has(a.name));
    const cacheablePool = [...responseWithImages, ...bufferMembers].slice(0, SIMILAR_POOL_SIZE);

    await storeCache(simCacheKey, "similar-artists", {
      baseArtist: foundName,
      seedTags,
      countryVerified: true,
    }, cacheablePool);
    await storeSimilarIndex(cacheablePool, simCacheKey);
    console.log(`[similar-artists] → ${foundName} (${responseWithImages.length} served / ${cacheablePool.length} cached)`);

    const _simResult = { baseArtist: foundName, artists: responseWithImages };
    if (_simResolve) { _simResolve(_simResult); similarInFlight.delete(_simKey); }
    res.json(_simResult);

    // Background: backfill images for remaining cached pool members so
    // future cache hits serve a richer set instantly.
    const needsBgImages = cacheablePool.some(a => !a.imageUrl);
    if (needsBgImages) {
      setImmediate(async () => {
        try {
          const patched = await ensureImages(cacheablePool);
          await storeCache(simCacheKey, "similar-artists", {
            baseArtist: foundName,
            seedTags,
            countryVerified: true,
          }, patched);
        } catch (err) {
          console.warn(`[similar-artists] bg image backfill failed: ${err?.message || err}`);
        }
      });
    }

    // Background: prime artist_countries for the top Last.fm similar artists
    // so future fresh-generation runs have the fast path ready.
    if (lastfmSimilar.length > 0 && mbQueue.length === 0) {
      const toSeed = [...lastfmSimilar].sort((a, b) => b.match - a.match).slice(0, 10);
      (async () => {
        let seeded = 0;
        for (const a of toSeed) {
          if (mbQueue.length > 3) break;
          const existing = await getMbArtistCached(a.name);
          if (existing !== undefined) continue;
          await mbArtistCountry(a.name);
          seeded++;
        }
        if (seeded > 0) console.log(`[similar-artists] seeded ${seeded} artist countries for "${foundName}" Last.fm pool`);
      })().catch(() => {});
    }
  } catch (err) {
    if (_simReject) { _simReject(err); similarInFlight.delete(_simKey); }
    console.error("Similar artists error:", err);
    res.status(500).json({ error: err.message });
  }
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
  ...new Set([
    ...Object.keys(COUNTRY_ISO),
    'Hawaii',
    // Historical civilizations
    'Soviet Union','Yugoslavia','Czechoslovakia','Ottoman Empire','East Germany','Ceylon','Rhodesia','Zaire',
    'Siam','Byzantine Empire','Prussia','Austro-Hungarian Empire','Ancient Rome','Ancient Greece',
    'Mesopotamia','Viking Scandinavia','Moorish Spain','Weimar Republic',
    'Republic of South Vietnam','Meiji Japan',
  ]),
];

const HISTORICAL_MUSIC_REGIONS = new Set([
  'Yugoslavia',
  'Soviet Union',
  'Czechoslovakia',
  'East Germany',
  'Ottoman Empire',
  'Ceylon',
  'Zaire',
  'Byzantine Empire',
  'Austro-Hungarian Empire',
  'Ancient Rome',
  'Ancient Greece',
  'Viking Scandinavia',
  'Moorish Spain',
  'Weimar Republic',
  'Meiji Japan',
  // Other legacy names already supported by enrichment.
  'Rhodesia',
  'Siam',
  'Prussia',
  'Mesopotamia',
  'Republic of South Vietnam',
]);

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

function canonicalizeArtistName(name) {
  if (!name) return name;
  let canonical = String(name).trim().replace(/\s+/g, " ");
  canonical = primaryArtistName(canonical)
    .replace(/\s+\((?:feat\.|ft\.|featuring)[^)]+\)$/i, "")
    .trim();

  // Track-credit style artist strings are poison for entity caches and country pools.
  // Only split on strong collaboration markers; avoid collapsing legitimate artist names.
  if (/\s*,\s*/.test(canonical) && /\b(?:feat\.|ft\.|featuring)\b/i.test(canonical)) {
    canonical = canonical.split(",")[0].trim();
  }
  if (/\s+\b(?:with|x)\b\s+/i.test(canonical) && /\b(?:feat\.|ft\.|featuring)\b/i.test(String(name))) {
    canonical = canonical.split(/\s+\b(?:with|x)\b\s+/i)[0].trim();
  }

  // If Claude returns a collab credit, keep the lead artist instead of storing the track-style string.
  if (canonical.includes(",")) {
    const parts = canonical.split(",").map(s => s.trim()).filter(Boolean);
    if (parts.length > 1 && /\b(?:feat\.|ft\.|featuring)\b/i.test(parts.slice(1).join(" "))) canonical = parts[0];
  }

  return canonical || name;
}

// Apply any name normalization to every artist object in a list.
function normalizeArtistNames(artists) {
  return artists
    .filter(a => a?.name)
    .map(a => ({ ...a, name: canonicalizeArtistName(a.name) }));
}

function normalizeArtistKey(name) {
  return (name || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function normalizeCountryName(name) {
  return (name || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

const COUNTRY_NAME_ALIASES = new Map([
  ["usa", "USA"],
  ["unitedstates", "USA"],
  ["us", "USA"],
  ["uk", "United Kingdom"],
  ["greatbritain", "United Kingdom"],
  ["britain", "United Kingdom"],
  ["uae", "UAE"],
  ["unitedarabemirates", "UAE"],
  ["ancientrome", "Ancient Rome"],
  ["ancientgreece", "Ancient Greece"],
  ["vikingscandinavia", "Viking Scandinavia"],
  ["moorishspain", "Moorish Spain"],
  ["weimarrepublic", "Weimar Republic"],
  ["meijijapan", "Meiji Japan"],
  ["sovietunion", "Soviet Union"],
  ["eastgermany", "East Germany"],
  ["ottomanempire", "Ottoman Empire"],
  ["byzantineempire", "Byzantine Empire"],
  ["austrohungarianempire", "Austro-Hungarian Empire"],
]);

function canonicalCountryName(name) {
  if (!name) return name;
  const trimmed = String(name).trim();
  if (ALL_ENRICHABLE_COUNTRIES.includes(trimmed)) return trimmed;
  const alias = COUNTRY_NAME_ALIASES.get(normalizeCountryName(trimmed));
  return alias || trimmed;
}

function historicalMusicRegionGuidance(country) {
  const canonical = canonicalCountryName(country);
  if (!HISTORICAL_MUSIC_REGIONS.has(canonical)) return "";
  return `Historical-region rule for ${canonical}: this is not a modern nationality bucket. Include real artists, ensembles, scholars, or tradition bearers who either (a) were active in or directly tied to the historical polity/period, (b) come from its successor regions and perform that specific tradition, or (c) make scholarly or historically grounded reconstructions of that tradition. For ancient civilizations, modern reconstruction ensembles are expected; do not imply they are literal ancient-era performers.`;
}

function mergeArtistPools(existingPool = [], incomingPool = [], limit = 80) {
  const merged = new Map();

  for (const artist of existingPool) {
    if (!artist?.name) continue;
    merged.set(normalizeArtistKey(artist.name), { ...artist });
  }

  for (const artist of incomingPool) {
    if (!artist?.name) continue;
    const key = normalizeArtistKey(artist.name);
    const previous = merged.get(key) || {};
    merged.set(key, {
      ...previous,
      ...artist,
      imageUrl: artist.imageUrl || previous.imageUrl,
      knownTracks: artist.knownTracks?.length ? artist.knownTracks : previous.knownTracks,
      hasVerifiedTracks: previous.hasVerifiedTracks || artist.hasVerifiedTracks || false,
    });
  }

  return [...merged.values()].slice(0, limit);
}

function dedupeArtistsByName(artists = [], limit = 100) {
  return [...artists.reduce((map, artist) => {
    if (!artist?.name) return map;
    const key = normalizeArtistKey(artist.name);
    const previous = map.get(key) || {};
    map.set(key, {
      ...previous,
      ...artist,
      imageUrl: artist.imageUrl || previous.imageUrl,
      knownTracks: artist.knownTracks?.length ? artist.knownTracks : previous.knownTracks,
      hasVerifiedTracks: previous.hasVerifiedTracks || artist.hasVerifiedTracks || false,
    });
    return map;
  }, new Map()).values()].slice(0, limit);
}

function isAmbiguousArtistName(name) {
  if (!name) return false;
  const trimmed = String(name).trim();
  const tokenCount = trimmed.split(/\s+/).filter(Boolean).length;
  const normalized = normalizeArtistKey(trimmed);
  if (!normalized) return false;
  if (/^[A-Z.&-]{2,5}$/.test(trimmed)) return true;
  return tokenCount === 1 && normalized.length <= 5;
}

function hasStrongArtistEvidence(meta, artist) {
  return !!(
    meta?.mbid ||
    meta?.countryCode ||
    (meta?.discogsYears?.length || 0) > 0 ||
    (artist?.knownTracks?.length || 0) >= 2 ||
    artist?.hasVerifiedTracks
  );
}

// Allow 1-character difference for names of similar length — handles transliteration
// variants like "Beqele" vs "Bekele" (Amharic q/k) without risking false positives.
function fuzzyArtistMatch(a, b) {
  if (a === b) return true;
  const lenDiff = Math.abs(a.length - b.length);
  if (lenDiff > 1 || Math.min(a.length, b.length) < 7) return false;
  if (lenDiff === 0) {
    // Same length: allow 1 substitution
    let diffs = 0;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i] && ++diffs > 1) return false;
    }
    return true;
  }
  // Lengths differ by 1: allow 1 insertion/deletion via subsequence check
  const [shorter, longer] = a.length < b.length ? [a, b] : [b, a];
  let si = 0, li = 0, skips = 0;
  while (si < shorter.length && li < longer.length) {
    if (shorter[si] === longer[li]) { si++; li++; }
    else { if (++skips > 1) return false; li++; }
  }
  return skips + (longer.length - li) <= 1;
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
        const stripParens = s => s.replace(/\s*\([^)]*\)/g, "").trim();
        const matched = artists.find(a => normalise(a.attributes?.name || "") === target)
          ?? artists.find(a => {
            const n = normalise(stripParens(a.attributes?.name || ""));
            return n === target || n.includes(target) || (n.length >= 6 && target.includes(n)) || fuzzyArtistMatch(n, target);
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
        const deduped = [...new Map(results.map(t => [t.appleId, t])).values()];
        if (deduped.length > 0) return deduped;
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
    const r = await lfFetch(
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
    const r = await lfFetch(
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
    const r = await lfFetch(
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
const DEEZER_UA = 'MusicalPassport/1.0 (+https://musicalpassport.app)';
const deezerQueue = [];
let deezerBusy = false;

// Circuit breaker: when Deezer's edge starts returning HTML error pages
// (common on shared Railway/egress IPs), stop hammering the API and flooding logs.
let deezerConsecutiveFailures = 0;
let deezerBlockedUntil = 0;
const DEEZER_BLOCK_THRESHOLD = 5;
const DEEZER_BLOCK_COOLDOWN_MS = 10 * 60 * 1000;

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

function deezerIsBlocked() {
  return Date.now() < deezerBlockedUntil;
}

function noteDeezerFailure(label, status, contentType) {
  deezerConsecutiveFailures++;
  if (deezerConsecutiveFailures === 1 || deezerConsecutiveFailures % 10 === 0) {
    console.warn(`  [${label}] non-JSON response (HTTP ${status || '?'}, ct=${contentType || 'none'}) — likely IP block`);
  }
  if (deezerConsecutiveFailures === DEEZER_BLOCK_THRESHOLD) {
    deezerBlockedUntil = Date.now() + DEEZER_BLOCK_COOLDOWN_MS;
    console.warn(`  [Deezer] cooling down for ${Math.round(DEEZER_BLOCK_COOLDOWN_MS / 60000)}m after ${deezerConsecutiveFailures} failures`);
  }
}

function noteDeezerSuccess() {
  if (deezerConsecutiveFailures > 0) console.log(`  [Deezer] recovered after ${deezerConsecutiveFailures} failures`);
  deezerConsecutiveFailures = 0;
  deezerBlockedUntil = 0;
}

// Fetches a Deezer endpoint and returns parsed JSON, or null if the response
// is not JSON / not OK / we are currently circuit-broken. Never throws.
async function deezerFetchJson(path, label = 'Deezer') {
  if (deezerIsBlocked()) return null;
  try {
    const r = await fetch(`${DEEZER_BASE}${path}`, {
      headers: { 'User-Agent': DEEZER_UA, 'Accept': 'application/json' },
    });
    const ct = r.headers.get('content-type') || '';
    if (!r.ok || !ct.includes('json')) {
      noteDeezerFailure(label, r.status, ct);
      return null;
    }
    const data = await r.json();
    noteDeezerSuccess();
    return data;
  } catch (err) {
    noteDeezerFailure(label, 0, `err:${err.message}`);
    return null;
  }
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
  const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const target = normalise(artistName);

  // Step 1: find the artist
  const searchData = await deezerEnqueue(() =>
    deezerFetchJson(`/search/artist?q=${encodeURIComponent(artistName)}&limit=5`, 'Deezer')
  );
  if (!searchData) return [];
  if (searchData.error?.code === 4) { console.warn('  [Deezer] quota exceeded'); return []; }

  const artists = searchData.data || [];
  const matched = artists.find(a => normalise(a.name) === target)
    ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || (n.length >= 5 && target.includes(n)); });

  if (!matched) { console.log(`  [Deezer] no artist match for "${artistName}"`); return []; }

  // Step 2: get top tracks
  const topData = await deezerEnqueue(() =>
    deezerFetchJson(`/artist/${matched.id}/top?limit=${limit}`, 'Deezer')
  );
  if (!topData) return [];
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
  const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const target = normalise(artistName);
  const d = await deezerEnqueue(() =>
    deezerFetchJson(`/search?q=artist:"${encodeURIComponent(artistName)}" track:"${encodeURIComponent(trackTitle)}"&limit=5`, 'Deezer')
  );
  if (!d || d.error?.code === 4) return null;
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
}

async function deezerRelatedArtists(artistName) {
  const normalise = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const target = normalise(artistName);

  const searchData = await deezerEnqueue(() =>
    deezerFetchJson(`/search/artist?q=${encodeURIComponent(artistName)}&limit=5`, 'Deezer related')
  );
  if (!searchData) return [];
  if (searchData.error?.code === 4) { console.warn('  [Deezer related] quota exceeded'); return []; }

  const artists = searchData.data || [];
  const matched = artists.find(a => normalise(a.name) === target)
    ?? artists.find(a => { const n = normalise(a.name); return n.includes(target) || (n.length >= 5 && target.includes(n)); });
  if (!matched) return [];

  const relData = await deezerEnqueue(() =>
    deezerFetchJson(`/artist/${matched.id}/related?limit=20`, 'Deezer related')
  );
  if (!relData || relData.error?.code === 4) return [];

  return (relData.data || []).map(a => a.name).filter(Boolean).slice(0, 20);
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
        return [...new Map(results.map(t => [t.spotifyId, t])).values()];
      })();
    }

    // Step 4: full Spotify artist-search path (top-tracks → track-search → LB → YouTube)
    // proactiveArtistTracks already tried Deezer + LB; this covers the Spotify angle
    // and enriches any LB results with YouTube preview URLs as a last resort
    if (tracks.length === 0) tracks = await proactiveSpotifyTracks(artistName);

    if (tracks.length > 0) {
      // Clear flag — we found tracks
      const storeEndpoint = isApple ? "artist-tracks-apple" : "artist-tracks";
      const tracksStripped = tracks.map(({ previewUrl, ...rest }) => rest);
      artistTracksMemCache.set(cacheKey, { tracks: tracksStripped, cachedAt: Date.now() });
      await storeCache(cacheKey, storeEndpoint, { tracks: tracksStripped });
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

const POOL_CAP = 80;
const POOL_GROW_TARGET = 3; // additions per enrichment run

// Tries to grow a country's artist pool toward POOL_CAP with verified additions.
// Sources: MusicBrainz + ListenBrainz (high/medium confidence) and Last.fm geo data.
// Asks Haiku to annotate candidates, then verifies each has playable tracks before adding.
async function growArtistPool(country, isoCode, currentPool, lfArtists, apiKey, appleToken) {
  const slots = Math.min(POOL_GROW_TARGET, POOL_CAP - currentPool.length);
  if (slots <= 0) return [];

  const normName = s => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  const currentNames = new Set(currentPool.map(a => normName(a.name)));

  // Gather candidates — MB+LB first (geographically verified by music databases),
  // then Last.fm geo (scrobbling-based, strong signal but looser geography).
  const candidates = [];
  if (isoCode) {
    try {
      const realPool = await buildRealArtistPool(country, isoCode);
      for (const a of realPool) {
        if (!currentNames.has(normName(a.name))) {
          candidates.push({ name: a.name, confidence: a.confidence });
        }
      }
    } catch {}
  }
  for (const name of lfArtists) {
    const n = normName(name);
    if (!currentNames.has(n) && !candidates.some(c => normName(c.name) === n)) {
      candidates.push({ name, confidence: "lastfm" });
    }
  }

  if (!candidates.length) {
    console.log(`[pool-grow] ${country}: no new candidates from databases`);
    return [];
  }

  // Prioritise: high (MB+LB) → medium-lb → medium-mb → lastfm
  const confRank = { high: 0, "medium-lb": 1, "medium-mb": 2, lastfm: 3 };
  candidates.sort((a, b) => (confRank[a.confidence] ?? 9) - (confRank[b.confidence] ?? 9));

  // Batch-annotate the top candidates with genre/era/similarTo via Haiku.
  // We ask Haiku because these artists ARE database-verified — we just need metadata.
  // Haiku is told to omit anyone it doesn't have real knowledge of.
  const topCandidates = candidates.slice(0, Math.min(slots * 5, 20));
  console.log(`[pool-grow] ${country}: ${candidates.length} candidates, annotating top ${topCandidates.length}…`);

  let annotated = [];
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 700,
        system: "You are a world music expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{
          role: "user",
          content: `These artists have been verified by MusicBrainz and/or ListenBrainz as being from ${country}.

Annotate each with genre, decade most active, and one well-known comparison artist.
ONLY include artists you have genuine knowledge of — omit anyone you are unsure about.
Do NOT repeat a name unless the repeated form is the artist's actual official name.

Artists: ${topCandidates.map(c => c.name).join(", ")}

Return JSON:
{
  "artists": [
    {
      "name": "Exact Name As Given",
      "genre": "specific local genre",
      "era": "1990s",
      "similarTo": "One well-known comparison artist"
    }
  ]
}

era must be exactly one of: 1900s, 1910s, 1920s, 1930s, 1940s, 1950s, 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s.`
        }],
      }),
    });
    const d = await res.json();
    if (!d.error) {
      const rawAnnotated = parseClaudeJson(d.content?.[0]?.text || "", `pool-grow ${country}`).artists || [];
      const filtered = rawAnnotated.filter(a => !isPlaceholderArtistName(a?.name));
      if (filtered.length < rawAnnotated.length) {
        console.log(`[pool-grow] ${country}: dropped ${rawAnnotated.length - filtered.length} placeholder name(s) from Haiku annotation`);
      }
      annotated = await applyArtistEvidence(filtered);
    }
  } catch (err) {
    console.error(`[pool-grow] ${country}: annotation failed —`, err.message);
    return [];
  }

  if (!annotated.length) {
    console.log(`[pool-grow] ${country}: Haiku had no knowledge of candidates`);
    return [];
  }

  // Re-filter after annotation — applyArtistEvidence can canonicalize names to match
  // artists already in the pool, or Haiku may return names not in the candidate list.
  const preVerifyCount = annotated.length;
  annotated = annotated.filter(a => !currentNames.has(normName(a.name)));
  if (annotated.length < preVerifyCount) {
    console.log(`[pool-grow] ${country}: dropped ${preVerifyCount - annotated.length} post-annotation duplicate(s) already in pool`);
  }
  if (!annotated.length) return [];

  const verifiedAnnotated = await verifyArtistMetadata(annotated, country, apiKey);
  const { validFlags: validMisattributed, recordsToRemove } = validateMisattributionFlags(verifiedAnnotated.misattributed, annotated, '[pool-grow]');
  if (validMisattributed.length > 0) {
    for (const m of validMisattributed) {
      console.log(`  [pool-grow] removed candidate ${m.name} → actually from ${m.actualCountry}`);
    }
    await routeMisattributedToCorrectCountry(validMisattributed, annotated, verifiedAnnotated.eraCorrections, country);
  }
  let growEraApplied = 0;
  annotated = annotated
    .filter(a => !recordsToRemove.has(a.name.toLowerCase()))
    .map(a => {
      const decision = decideEraApply(a, verifiedAnnotated.eraCorrections, verifiedAnnotated.highConfidenceEras);
      if (!decision) return a;
      console.log(`  [pool-grow] era fix: ${a.name} ${a.era} → ${decision.era}${decision.evidenceBacked ? ' (evidence-backed)' : ''}`);
      if (decision.evidenceBacked) persistEraFix(a.name, decision.era);
      growEraApplied++;
      return { ...a, era: decision.era };
    });
  if (growEraApplied > 0 || verifiedAnnotated.eraCorrections.size > 0) {
    console.log(`[pool-grow] ${country}: applied ${growEraApplied}/${verifiedAnnotated.eraCorrections.size} era fix(es) in candidate batch`);
  }
  if (!annotated.length) {
    console.log(`[pool-grow] ${country}: no verified candidates remained after metadata check`);
    return [];
  }

  // Verify each annotated artist has playable tracks before adding to pool
  const added = [];
  for (const artist of annotated) {
    if (added.length >= slots) break;

    const ck = makeCacheKey(["artist-tracks-apple", artist.name]);
    const existing = await getCached(ck);
    let tracks = existing?.result?.tracks || [];

    if (tracks.length) {
      console.log(`  [pool-grow] ↩ ${artist.name} → ${tracks.length} tracks (cached)`);
    } else {
      const lfTitles = await lastfmArtistTopTracks(artist.name, 5);
      tracks = await proactiveArtistTracks(artist.name, lfTitles, appleToken);
      if (tracks.length) {
        artistTracksMemCache.set(ck, { tracks, cachedAt: Date.now() });
        await storeCache(ck, "artist-tracks-apple", { tracks });
      }
    }

    if (!tracks.length) {
      console.log(`  [pool-grow] – ${artist.name}: no tracks found`);
      await new Promise(r => setTimeout(r, 200));
      continue;
    }

    const imageUrl = await fetchArtistImageUrl(artist.name, { genre: artist.genre }).catch(() => null);
    added.push({ ...artist, hasVerifiedTracks: true, ...(imageUrl ? { imageUrl } : {}) });
    console.log(`  [pool-grow] ✓ ${artist.name} (${artist.era} ${artist.genre})`);
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`[pool-grow] ${country}: added ${added.length}/${slots} artist(s)`);
  return added;
}

async function verifyArtistMetadata(artists, country, apiKey) {
  if (!artists?.length) return { misattributed: [], eraCorrections: new Map() };
  country = canonicalCountryName(country);
  const historicalGuidance = historicalMusicRegionGuidance(country);

  const artistsWithEvidence = await applyArtistEvidence(artists);
  const evidenceByName = new Map();
  for (const artist of artistsWithEvidence) {
    const meta = await getArtistMetaEvidence(artist.name);
    evidenceByName.set(artist.name.toLowerCase(), meta);
  }

  const artistsBlock = artists
    .map(a => {
      const evidence = evidenceByName.get(canonicalizeArtistName(a.name).toLowerCase());
      const currentEra = normalizeDecade(a.era) || "unknown";
      const evidenceEra = normalizeDecade(evidence?.inferredEra) || "unknown";
      return `- ${canonicalizeArtistName(a.name)} | currentEra=${currentEra} | evidenceEra=${evidenceEra}`;
    })
    .join("\n");

  let result;
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1200,
        system: "You are a music geography and music history expert. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
        messages: [{
          role: "user",
          content: `These artists are currently stored for "${country}".

For each artist, verify two things:
1. ${historicalGuidance ? `Do they genuinely belong in the ${country} historical/reconstruction tradition described below?` : `Are they genuinely from ${country}?`}
2. If they belong here, is their currentEra the correct decade they are MOST associated with or most active in?

${historicalGuidance}

Artists to verify:
${artistsBlock}

Rules:
- Only mark an artist as misattributed if they clearly do not fit the stated geographic, historical, or reconstruction scope.
- For modern countries, diaspora artists, dual-heritage artists, or artists whose musical identity remains rooted in ${country} should stay.
- For historical regions, modern reconstruction ensembles and scholars should stay when their work is specifically rooted in that historical tradition.
- Soviet-era artists belong to their birth republic, not USSR/Russia, unless they are clearly rooted elsewhere.
- For era corrections, only return a correction when the currentEra is clearly wrong.
- era must be exactly one of: ${DECADES_LIST.join(", ")}.
- Do not guess. If uncertain, omit the artist from both arrays.

Return JSON:
{
  "misattributed": [
    { "name": "Exact Artist Name", "actualCountry": "Country Name" }
  ],
  "eraCorrections": [
    { "name": "Exact Artist Name", "era": "1970s" }
  ]
}`
        }],
      }),
    });
    const data = await res.json();
    if (data.error) {
      console.error(`[artist-verify] Claude error for ${country}: ${data.error.message}`);
      return { misattributed: [], eraCorrections: new Map() };
    }
    result = parseClaudeJson(data.content?.[0]?.text || "", `artist-verify ${country}`);
  } catch (err) {
    console.error(`[artist-verify] ${country}: failed —`, err.message);
    return { misattributed: [], eraCorrections: new Map() };
  }

  const currentCountryNorm = normalizeCountryName(country);
  // Each misattributed item is tagged internally with `source` so we can apply
  // different downstream policies: MB-derived flags are deterministic and trusted;
  // Claude-only flags get a tiebreaker pass before we act on them. Source is
  // stripped before returning so callers see a clean shape.
  const misattributed = (result.misattributed || [])
    .filter(a => {
      if (!a?.name || !a?.actualCountry) return false;
      const actualCountry = String(a.actualCountry).trim();
      if (!actualCountry) return false;
      if (/[()/]/.test(actualCountry) || /\bmixed attribution\b/i.test(actualCountry)) return false;
      if (normalizeCountryName(actualCountry) === currentCountryNorm) return false;
      return true;
    })
    .map(a => ({ ...a, source: "claude" }));

  // Deterministic guardrail: if MusicBrainz has a country code on file for an
  // artist and it disagrees with the target country, treat that as authoritative
  // — Claude (the same model that may have invented the misattribution) can miss
  // these blind spots (e.g. proposing US artist "sombr" for Latvia).
  const targetIso = COUNTRY_ISO[country] || null;
  if (targetIso) {
    const flagByName = new Map(misattributed.map(m => [m.name.toLowerCase(), m]));
    for (const artist of artistsWithEvidence) {
      const evidence = evidenceByName.get(artist.name.toLowerCase());
      const mbIso = evidence?.countryCode;
      if (!mbIso || mbIso === targetIso) continue;
      const actualCountry = ISO_TO_COUNTRY[mbIso];
      if (!actualCountry || normalizeCountryName(actualCountry) === currentCountryNorm) continue;
      const existing = flagByName.get(artist.name.toLowerCase());
      if (existing) {
        // Claude and MB agree → upgrade to MB-source (deterministic) and prefer MB's actualCountry mapping.
        existing.source = "mb";
        existing.actualCountry = actualCountry;
      } else {
        const flag = { name: artist.name, actualCountry, source: "mb" };
        misattributed.push(flag);
        flagByName.set(artist.name.toLowerCase(), flag);
      }
    }
  }

  // Strong rejection: if MB explicitly confirms the artist IS from the target country,
  // Claude's misattribution flag is almost certainly wrong — drop it before any tiebreaker.
  if (targetIso) {
    for (let i = misattributed.length - 1; i >= 0; i--) {
      const m = misattributed[i];
      if (m.source !== "claude") continue;
      const ev = evidenceByName.get(m.name.toLowerCase());
      if (ev?.countryCode === targetIso) {
        console.log(`  [artist-verify] MB confirms "${m.name}" is from ${country} — rejecting Claude's misattribution flag`);
        misattributed.splice(i, 1);
      }
    }
  }

  // Opus tiebreaker — Claude-only flags where MB has no country evidence are the
  // borderline case. Default to KEEPING the artist; Opus must explicitly confirm removal.
  const borderline = misattributed.filter(m => {
    if (m.source !== "claude") return false;
    const ev = evidenceByName.get(m.name.toLowerCase());
    return !ev?.countryCode;
  });
  if (borderline.length > 0) {
    const confirmed = await opusMisattributionTiebreaker(borderline, country, apiKey, historicalGuidance);
    let kept = 0;
    for (let i = misattributed.length - 1; i >= 0; i--) {
      const m = misattributed[i];
      if (m.source !== "claude") continue;
      const ev = evidenceByName.get(m.name.toLowerCase());
      if (ev?.countryCode) continue; // not borderline
      if (!confirmed.has(m.name.toLowerCase())) {
        console.log(`  [artist-verify] tiebreaker kept "${m.name}" in ${country} pool (no MB evidence + Opus did not confirm removal)`);
        misattributed.splice(i, 1);
        kept++;
      }
    }
    console.log(`[artist-verify] ${country}: tiebreaker reviewed ${borderline.length} borderline → ${confirmed.size} removed, ${kept} kept`);
  }
  const eraCorrections = new Map();
  // highConfidenceEras: only eras backed by real MB/Discogs evidence — these are safe to persist
  // as era_verified=true. Claude's opinion (below) is non-deterministic and must NOT mark verified.
  const highConfidenceEras = new Map();
  for (const artist of artistsWithEvidence) {
    const evidence = evidenceByName.get(artist.name.toLowerCase());
    const evidenceEra = normalizeDecade(evidence?.inferredEra);
    if (!evidenceEra) continue;
    if (evidence?.eraConfidence === "high") {
      highConfidenceEras.set(artist.name.toLowerCase(), evidenceEra);
    }
    if (evidence?.eraConfidence === "high" || !normalizeDecade(artist.era)) {
      if (evidenceEra !== normalizeDecade(artist.era)) eraCorrections.set(artist.name.toLowerCase(), evidenceEra);
    }
  }
  for (const correction of (result.eraCorrections || [])) {
    const era = normalizeDecade(correction?.era);
    if (correction?.name && era) eraCorrections.set(correction.name.toLowerCase(), era);
  }
  // Strip internal `source` tag so callers see a clean shape.
  const cleanMisattributed = misattributed.map(({ source, ...rest }) => rest);
  return { misattributed: cleanMisattributed, eraCorrections, highConfidenceEras };
}

// Filters out placeholder strings that occasionally leak in when an LLM tries to fill a quota
// it can't satisfy (e.g. "[unknown]", "Unknown Artist", "Various Artists"). These get cached as
// real artist rows and reappear in subsequent runs, polluting the pool. Be conservative — bands
// like "Unknown Mortal Orchestra" are real, so match precise placeholder shapes only.
function isPlaceholderArtistName(name) {
  if (!name || typeof name !== "string") return true;
  const trimmed = name.trim();
  if (!trimmed) return true;
  if (/^\[.*\]$/.test(trimmed)) return true;                 // [unknown], [artist], [n/a]
  if (/^unknown(\s+artist)?$/i.test(trimmed)) return true;   // "Unknown" / "Unknown Artist"
  if (/^various\s+artists?$/i.test(trimmed)) return true;    // "Various Artists"
  if (/^artist(\s*\d+)?$/i.test(trimmed)) return true;       // "Artist", "Artist 1"
  if (/^n\/?a$/i.test(trimmed)) return true;                 // "N/A", "NA"
  return false;
}

// Decides whether to apply a proposed era correction to an artist record. Returns
// { era, evidenceBacked } when the change should land, or null when it should be skipped.
//
// Claude is non-deterministic about era guesses; if we accept its opinion every run the
// stored era oscillates (1980s → 1970s → 1980s …) when there's no MB/Discogs ground truth.
// Rule:
//   - If high-confidence evidence (MB/Discogs) backs the change → apply (and `evidenceBacked: true`
//     so the caller can persist `era_verified=true`).
//   - If the artist has no era at all yet → apply Claude's guess so the field gets populated.
//   - Otherwise (overwriting a non-empty era with a low-confidence Claude opinion) → skip.
function decideEraApply(artist, eraCorrections, highConfidenceEras) {
  const correctedEra = eraCorrections?.get?.(artist.name.toLowerCase());
  if (!correctedEra || correctedEra === artist.era) return null;
  const hcEra = highConfidenceEras?.get?.(artist.name.toLowerCase());
  const evidenceBacked = !!(hcEra && hcEra === correctedEra);
  const currentEraIsValid = !!normalizeDecade(artist.era);
  if (evidenceBacked || !currentEraIsValid) return { era: correctedEra, evidenceBacked };
  return null;
}

// Locates an artist record in a pool, falling back to a fuzzy key (lowercase + stripped
// of "The " prefix and non-alphanumerics) when exact match misses. Returns the record or null.
// Use this whenever you want to act on a name that may have been canonicalized between the
// pool's stored form and the form returned by `applyArtistEvidence`/Claude.
function findArtistInPool(pool, name) {
  if (!Array.isArray(pool) || !name) return null;
  const lower = String(name).toLowerCase();
  const exact = pool.find(a => a?.name?.toLowerCase() === lower);
  if (exact) return exact;
  const fuzzyKey = s => normalizeArtistKey(String(s || "").replace(/^the\s+/i, ""));
  const target = fuzzyKey(name);
  if (!target) return null;
  return pool.find(a => fuzzyKey(a?.name) === target) || null;
}

// Routes misattributed artists into the correct country's cached pool.
// - Skips when the destination country is not enrichable.
// - Skips when the destination has no cached pool (would orphan the artist).
// - Skips when the artist already exists in the destination pool (no duplicates).
// Caller is still responsible for removing the artist from the source pool.
async function routeMisattributedToCorrectCountry(misattributed, sourcePool, eraCorrections, fromCountry) {
  if (!misattributed?.length) return { moved: 0, skipped: 0 };
  let moved = 0;
  let skipped = 0;
  for (const { name, actualCountry } of misattributed) {
    if (!name || !actualCountry) { skipped++; continue; }
    const dest = canonicalCountryName(actualCountry);
    if (!ALL_ENRICHABLE_COUNTRIES.includes(dest)) {
      console.log(`  [reroute] ${name}: actualCountry "${actualCountry}" not in enrichable list — dropped from ${fromCountry}`);
      skipped++;
      continue;
    }
    if (normalizeCountryName(dest) === normalizeCountryName(fromCountry)) {
      skipped++;
      continue;
    }
    const artist = findArtistInPool(sourcePool, name);
    if (!artist) {
      console.log(`  [reroute] ${name}: not found in source pool for ${fromCountry} — dropped (likely name canonicalization mismatch)`);
      skipped++;
      continue;
    }

    const destKey = makeCacheKey(["recommend", dest]);
    const destCache = await getCached(destKey);
    if (!destCache?.artist_pool) {
      console.log(`  [reroute] ${name}: no cache for ${dest} yet — dropped from ${fromCountry}`);
      skipped++;
      continue;
    }
    if (findArtistInPool(destCache.artist_pool, name)) {
      console.log(`  [reroute] ${name} already in ${dest} pool — dropped from ${fromCountry}`);
      skipped++;
      continue;
    }
    const correctedEra = eraCorrections?.get(name.toLowerCase());
    const artistToInsert = { ...artist, ...(correctedEra ? { era: correctedEra } : {}) };
    await storeCache(destKey, "recommend", destCache.result, [...destCache.artist_pool, artistToInsert]);
    console.log(`  [reroute] ✓ ${name} → ${dest} (from ${fromCountry})`);
    moved++;
  }
  return { moved, skipped };
}

// Filters a misattribution list to only flags whose target artist is actually present in the
// pool. Returns { validFlags, recordsToRemove } where:
//   - validFlags: the misattribution entries we can safely act on
//   - recordsToRemove: a Set of lowercased artist *record* names suitable for use in a pool filter
// The record-name set is what removes the *real* artist (e.g. "E Street Band") even when the
// flag uses a canonicalized form ("The E Street Band"). Hallucinated flags (Claude saying
// "Richard Thompson" when the pool only has "Thompson") are dropped with a debug log.
function validateMisattributionFlags(misattributed, sourcePool, logPrefix) {
  const validFlags = [];
  const recordsToRemove = new Set();
  for (const m of misattributed || []) {
    const record = findArtistInPool(sourcePool, m?.name);
    if (record) {
      validFlags.push(m);
      recordsToRemove.add(record.name.toLowerCase());
    } else {
      console.log(`  ${logPrefix} ignored misattribution flag for "${m?.name}" — not in source pool by exact or fuzzy match (likely Claude hallucination)`);
    }
  }
  return { validFlags, recordsToRemove };
}

// Opus tiebreaker — for artists Claude flagged as misattributed but where MusicBrainz has no country
// evidence to corroborate. Conservative by design: only confirm removal when highly confident.
// One Claude Opus call per invocation, batched up to 8 items, to stay friendly with rate limits.
async function opusMisattributionTiebreaker(items, country, apiKey, historicalGuidance) {
  if (!items?.length || !apiKey) return new Set();
  const batch = items.slice(0, 8);
  const list = batch.map(m => `- ${m.name} (first-pass verifier said actually from: ${m.actualCountry})`).join("\n");
  let parsed;
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-opus-4-5-20251101",
        max_tokens: 800,
        system: "You are a world music geography and history expert. Be conservative — only confirm misattribution when you are highly confident the artist does NOT belong to the stated country. When uncertain, REJECT the flag (i.e., keep them). Return ONLY valid JSON — no markdown, no preamble.",
        messages: [{
          role: "user",
          content: `These artists are stored as being from "${country}". A first-pass verifier flagged them as misattributed, but MusicBrainz has no country evidence either way — this is the borderline case.

${historicalGuidance ? historicalGuidance + "\n" : ""}For each artist, decide: should they be REMOVED from ${country}'s pool?
- Only confirm removal if you are highly confident they are NOT from ${country} (or its historical/reconstruction tradition).
- Diaspora artists, dual-heritage artists, or artists whose musical identity remains rooted in ${country} should NOT be removed.
- If you are unsure or the artist could plausibly belong here, REJECT the removal.
- Soviet-era artists belong to their birth republic, not USSR/Russia, unless they are clearly rooted elsewhere.

Artists to review:
${list}

Return JSON:
{
  "confirmedRemovals": [
    { "name": "Exact Name", "reason": "brief reason" }
  ]
}
Only include artists you are confident should be removed. Omit any you are uncertain about.`
        }],
      }),
    });
    const data = await res.json();
    if (data.error) {
      console.error(`[tiebreaker] Opus error for ${country}: ${data.error.message}`);
      return new Set();
    }
    parsed = parseClaudeJson(data.content?.[0]?.text || "", `tiebreaker ${country}`);
  } catch (err) {
    console.error(`[tiebreaker] ${country} failed —`, err.message);
    return new Set();
  }
  const confirmed = new Set();
  for (const c of (parsed.confirmedRemovals || [])) {
    if (c?.name) {
      confirmed.add(c.name.toLowerCase());
      console.log(`  [tiebreaker] confirmed removal: ${c.name} — ${c.reason || ''}`);
    }
  }
  return confirmed;
}

// Audits the existing artist_pool for a country, identifying misattributed artists
// and correcting any clearly wrong era tags in place.
async function auditCountryPool(artistPool, country, apiKey) {
  if (!artistPool?.length) return { removedNames: [], correctedPool: artistPool, changed: false };

  // Strip placeholder names from existing pools — historical entries like "[unknown]" or
  // "Various Artists" leaked in before the boundary filter existed. The rotating cron audit
  // sweeps every country over time, so this gradually cleans them all up.
  const beforePlaceholderFilter = artistPool.length;
  let poolToAudit = artistPool.filter(a => !isPlaceholderArtistName(a?.name));
  const placeholdersDropped = beforePlaceholderFilter - poolToAudit.length;
  if (placeholdersDropped > 0) {
    console.log(`[pool-audit] ${country}: dropped ${placeholdersDropped} placeholder name(s) from existing pool`);
  }
  if (!poolToAudit.length) {
    return { removedNames: [], correctedPool: poolToAudit, changed: placeholdersDropped > 0 };
  }

  // Pre-apply any previously-verified eras so the audit starts from a clean baseline.
  // Verified artists won't have their eras re-corrected by the AI below.
  const preVerifiedEras = await getVerifiedEras(poolToAudit.map(a => a.name));
  if (preVerifiedEras.size > 0) {
    poolToAudit = poolToAudit.map(a => {
      const ve = preVerifiedEras.get(a.name.toLowerCase());
      return ve && ve !== a.era ? { ...a, era: ve } : a;
    });
  }

  console.log(`[pool-audit] ${country}: auditing ${poolToAudit.length} artists…`);

  const { misattributed: rawMisattributed, eraCorrections, highConfidenceEras } = await verifyArtistMetadata(poolToAudit, country, apiKey);
  // Drop misattribution flags that don't actually point to anything in the pool — these are
  // most often Claude hallucinating a longer/different canonical name from a short pool entry
  // (e.g. flagging "Richard Thompson" when the pool only contains the Croatian band "Thompson").
  const { validFlags: misattributed, recordsToRemove } = validateMisattributionFlags(rawMisattributed, poolToAudit, '[pool-audit]');
  let eraApplied = 0;
  const applyEra = (artist) => {
    if (preVerifiedEras.has(artist.name.toLowerCase())) return artist; // era already verified — never overwrite
    const decision = decideEraApply(artist, eraCorrections, highConfidenceEras);
    if (!decision) return artist;
    console.log(`  [pool-audit] era fix: ${artist.name} ${artist.era} → ${decision.era}${decision.evidenceBacked ? ' (evidence-backed)' : ''}`);
    if (decision.evidenceBacked) persistEraFix(artist.name, decision.era);
    eraApplied++;
    return { ...artist, era: decision.era };
  };

  if (!misattributed.length) {
    if (eraCorrections.size === 0) {
      console.log(`[pool-audit] ${country}: artists and eras verified ✓`);
      return { removedNames: [], correctedPool: poolToAudit, changed: preVerifiedEras.size > 0 || placeholdersDropped > 0 };
    }
    const correctedPool = poolToAudit.map(applyEra);
    if (eraApplied > 0 || eraCorrections.size > 0) {
      console.log(`[pool-audit] ${country}: applied ${eraApplied}/${eraCorrections.size} era fix(es)`);
    }
    return { removedNames: [], correctedPool, changed: eraApplied > 0 || placeholdersDropped > 0 };
  }

  console.log(`[pool-audit] ${country}: ${misattributed.length} misattributed — ${misattributed.map(m => `${m.name} → ${m.actualCountry}`).join(", ")}`);
  await routeMisattributedToCorrectCountry(misattributed, poolToAudit, eraCorrections, country);

  const correctedPool = poolToAudit
    .filter(artist => !recordsToRemove.has(artist.name.toLowerCase()))
    .map(applyEra);

  const removedNames = [...recordsToRemove];
  if (eraApplied > 0 || eraCorrections.size > 0) {
    console.log(`[pool-audit] ${country}: applied ${eraApplied}/${eraCorrections.size} era fix(es)`);
  }
  if (removedNames.length > 0) {
    console.log(`[pool-audit] ${country}: removed ${removedNames.length} artist(s) from pool`);
  }

  return { removedNames, correctedPool, changed: removedNames.length > 0 || eraApplied > 0 || placeholdersDropped > 0 };
}

// Picks the N least-recently-audited country pools and re-runs auditCountryPool on each.
// Tracks audit recency in result.lastAuditedAt so we cycle through every country over time.
// Each country: 1 Haiku call + (≤1 Opus tiebreaker) + queue-throttled MB/Discogs lookups.
// 2-second pause between countries keeps Anthropic rate limits comfortable.
async function rotateCountryPoolAudit(apiKey, count = 3) {
  if (!supabase || !apiKey) return { audited: 0, removed: 0 };

  const keyToCountry = {};
  const cacheKeys = [];
  for (const country of [...new Set(Object.keys(COUNTRY_ISO))]) {
    const key = makeCacheKey(["recommend", country]);
    keyToCountry[key] = country;
    cacheKeys.push(key);
  }

  const { data: rows, error } = await supabase
    .from("recommendation_cache")
    .select("cache_key, result, artist_pool")
    .eq("endpoint", "recommend")
    .in("cache_key", cacheKeys)
    .not("artist_pool", "is", null);
  if (error) {
    console.error("[rotate-audit] query error:", error.message);
    return { audited: 0, removed: 0 };
  }

  // Sort by lastAuditedAt ASC, NULLs first so never-audited pools go to the front.
  const sorted = (rows || [])
    .filter(r => Array.isArray(r.artist_pool) && r.artist_pool.length > 0)
    .sort((a, b) => {
      const ax = a.result?.lastAuditedAt;
      const bx = b.result?.lastAuditedAt;
      if (!ax && !bx) return 0;
      if (!ax) return -1;
      if (!bx) return 1;
      return ax.localeCompare(bx);
    })
    .slice(0, count);

  if (!sorted.length) return { audited: 0, removed: 0 };

  let audited = 0, removed = 0;
  for (const row of sorted) {
    const country = keyToCountry[row.cache_key];
    if (!country) continue;
    try {
      console.log(`[rotate-audit] starting ${country} (last audited: ${row.result?.lastAuditedAt || 'never'})`);
      const audit = await auditCountryPool(row.artist_pool, country, apiKey);
      const newResult = { ...(row.result || {}), lastAuditedAt: new Date().toISOString() };
      // Always touch lastAuditedAt — even when nothing changed — so the rotation moves on next run.
      const poolToStore = audit.changed ? audit.correctedPool : row.artist_pool;
      await storeCache(row.cache_key, "recommend", newResult, poolToStore);
      if (audit.changed) {
        console.log(`[rotate-audit] ${country}: ${audit.removedNames.length} removed, pool ${row.artist_pool.length} → ${audit.correctedPool.length}`);
        removed += audit.removedNames.length;
      }
      audited++;
    } catch (err) {
      console.error(`[rotate-audit] ${country} failed:`, err.message);
    }
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log(`[rotate-audit] done: ${audited} audited, ${removed} artists removed`);
  return { audited, removed };
}

async function deepEnrichCountry(country, apiKey) {
  country = canonicalCountryName(country);
  const historicalGuidance = historicalMusicRegionGuidance(country);
  console.log(`[country-enrich] Researching ${country}...`);
  const appleToken = generateAppleMusicToken();

  // Step 0: Audit existing pool — remove misattributed artists and move them to the right country
  const cacheKey = makeCacheKey(["recommend", country]);
  const existingCache = await getCached(cacheKey);
  let currentPool = existingCache?.artist_pool || [];
  if (existingCache?.artist_pool?.length) {
    const audit = await auditCountryPool(existingCache.artist_pool, country, apiKey);
    if (audit.changed) {
      await storeCache(cacheKey, "recommend", existingCache.result, audit.correctedPool);
      currentPool = audit.correctedPool;
    }
  }

  // Step 1a: Pull Last.fm geo.getTopArtists to ground Claude in real scrobble data
  const lfArtists = await lastfmGeoTopArtists(country, 25);
  const lfNote = lfArtists.length > 0
    ? historicalGuidance
      ? `\n\nLast.fm top artists for ${country} (global scrobbling signal — treat as candidates, then apply the historical-region rule):\n${lfArtists.join(', ')}\n\nPrioritize these artists only when they fit ${country}'s historical tradition or reconstruction lineage.`
      : `\n\nLast.fm top artists for ${country} (verified by global scrobbling data — these artists are genuinely associated with ${country}):\n${lfArtists.join(', ')}\n\nPrioritize these artists in your response. You may add others not on this list only if you are certain they are from ${country}.`
    : '';
  if (lfArtists.length > 0) console.log(`[country-enrich] Last.fm → ${lfArtists.length} artists for ${country}`);

  // Step 1b: Claude researches the country's music scene in depth
  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({
      model: "claude-opus-4-5-20251101",
      max_tokens: 3500,
      system: "You are a world music ethnomusicologist with encyclopedic knowledge of music from every country and historical region. Return ONLY valid JSON — no markdown, no backticks, no preamble.",
      messages: [{
        role: "user",
        content: `Deep music research for "${country}".

${historicalGuidance || `Every artist MUST be genuinely from ${country} — born there or the group formed there. No exceptions.`}

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
  // Drop placeholder names ("[unknown]", "Various Artists", etc) before any further work —
  // otherwise they get cached, treated as real artists, and reappear in subsequent runs.
  const beforePlaceholderFilter = research.artists?.length || 0;
  research.artists = (research.artists || []).filter(a => !isPlaceholderArtistName(a?.name));
  if (research.artists.length < beforePlaceholderFilter) {
    console.log(`[country-enrich] ${country}: dropped ${beforePlaceholderFilter - research.artists.length} placeholder name(s) from Claude research`);
  }
  research.artists = await applyArtistEvidence(research.artists);
  const verifiedResearch = await verifyArtistMetadata(research.artists, country, apiKey);
  const { validFlags: researchValidMisattributed, recordsToRemove: researchRecordsToRemove } =
    validateMisattributionFlags(verifiedResearch.misattributed, research.artists, '[country-enrich]');
  // Reroute misattributed artists into the correct country's pool BEFORE we filter them out,
  // so the helper can still find their original records in research.artists.
  if (researchValidMisattributed.length > 0) {
    await routeMisattributedToCorrectCountry(researchValidMisattributed, research.artists, verifiedResearch.eraCorrections, country);
  }
  let researchEraApplied = 0;
  research.artists = research.artists
    .filter(a => !researchRecordsToRemove.has(a.name.toLowerCase()))
    .map(a => {
      const decision = decideEraApply(a, verifiedResearch.eraCorrections, verifiedResearch.highConfidenceEras);
      if (!decision) return a;
      console.log(`  [country-enrich] era fix: ${a.name} ${a.era} → ${decision.era}${decision.evidenceBacked ? ' (evidence-backed)' : ''}`);
      if (decision.evidenceBacked) persistEraFix(a.name, decision.era);
      researchEraApplied++;
      return { ...a, era: decision.era };
    });
  if (researchValidMisattributed.length > 0) {
    console.log(`[country-enrich] ${country}: removed ${researchValidMisattributed.length} misattributed artist(s) from fresh research`);
    for (const m of researchValidMisattributed) {
      console.log(`  [country-enrich] removed ${m.name} → actually from ${m.actualCountry}`);
    }
  }
  if (researchEraApplied > 0 || verifiedResearch.eraCorrections.size > 0) {
    console.log(`[country-enrich] ${country}: applied ${researchEraApplied}/${verifiedResearch.eraCorrections.size} era fix(es) from fresh research`);
  }

  console.log(`[country-enrich] ${country}: ${research.artists.length} artists from Claude`);
  for (const a of research.artists) {
    console.log(`  [country-enrich] Claude: ${a.name} (${a.era}, ${a.genre})`);
  }

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
  const existingImageMap = {};
  for (const a of currentPool) {
    if (a.name && a.imageUrl) existingImageMap[a.name.toLowerCase()] = a.imageUrl;
  }

  const researchArtistsWithImages = await Promise.all(research.artists.map(async (a) => {
    const existing = existingImageMap[a.name.toLowerCase()];
    if (existing) return { ...a, imageUrl: existing };
    const imageUrl = await fetchArtistImageUrl(a.name, { genre: a.genre }).catch(() => null);
    return imageUrl ? { ...a, imageUrl } : a;
  }));
  console.log(`[country-enrich] ${country}: fetched images for ${researchArtistsWithImages.filter(a => a.imageUrl).length}/${researchArtistsWithImages.length} artists`);

  // Apply any previously-verified eras from artist_metadata before merging —
  // prevents Claude / Last.fm from overwriting a confirmed correction on the next run.
  const verifiedEraMap = await getVerifiedEras(
    [...currentPool, ...researchArtistsWithImages].map(a => a.name)
  );
  const applyVerifiedEra = a => {
    const ve = verifiedEraMap.get(a.name.toLowerCase());
    return ve && ve !== a.era ? { ...a, era: ve } : a;
  };
  const poolForMerge  = verifiedEraMap.size > 0 ? currentPool.map(applyVerifiedEra)              : currentPool;
  const freshForMerge = verifiedEraMap.size > 0 ? researchArtistsWithImages.map(applyVerifiedEra) : researchArtistsWithImages;

  const artistsWithImages = mergeArtistPools(poolForMerge, freshForMerge);
  console.log(`[country-enrich] ${country}: merged pool base ${currentPool.length} + fresh ${researchArtistsWithImages.length} → ${artistsWithImages.length}`);

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
      console.log(`  [country-enrich] ↩ ${artist.name} → ${existing.result.tracks.length} tracks (cached)`);
      successfulArtists.push(artist);
      continue;
    }

    const tracks = await proactiveArtistTracks(artist.name, artist.knownTracks || [], appleToken);

    if (tracks.length > 0) {
      const tracksStripped = tracks.map(({ previewUrl, ...rest }) => rest);
      artistTracksMemCache.set(artistCacheKey, { tracks: tracksStripped, cachedAt: Date.now() });
      await storeCache(artistCacheKey, "artist-tracks-apple", { tracks: tracksStripped });
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
  const finalPool = [
    ...successfulArtists,
    ...replacements,
    ...failedArtists.slice(replacements.length),
  ];
  if (replacements.length > 0) {
    await storeCache(cacheKey, "recommend",
      { genres: research.genres, didYouKnow: research.didYouKnow, streamingFloor },
      finalPool
    );
    console.log(`[country-enrich] ${country}: updated pool with ${replacements.length} replacements`);
  }

  // Step 5: Grow the pool toward 40 with database-verified additions
  const isoCode = COUNTRY_ISO[country];
  const growAdditions = await growArtistPool(country, isoCode, finalPool, lfArtists, apiKey, appleToken);
  if (growAdditions.length) {
    await storeCache(cacheKey, "recommend",
      { genres: research.genres, didYouKnow: research.didYouKnow, streamingFloor },
      [...finalPool, ...growAdditions]
    );
  }

  const withTracks = successfulArtists.length + replacements.length + growAdditions.length;
  const withoutTracks = failedArtists.length - replacements.length;
  console.log(`[country-enrich] ${country} done: ${withTracks} with tracks, ${withoutTracks} without, pool size ${finalPool.length + growAdditions.length}`);
  return { country, artists: research.artists.length, withTracks, withoutTracks, poolSize: finalPool.length + growAdditions.length };
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
      const targetCountry = canonicalCountryName(req.query.country);
      targets = [targetCountry];
      console.log(`[country-enrich] Targeting specific country: ${targetCountry}`);
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
//
// Accepts an optional ?startDate=YYYY-MM-DD so the client can pin the rotation
// to its local "today" instead of letting the server fall back to UTC. Without
// this, users east of UTC see the country roll a few hours before midnight.
app.get("/api/country-of-day/upcoming", async (req, res) => {
  const days = Math.min(parseInt(req.query.days || "7", 10), 30);
  const startDate = (req.query.startDate || "").match(/^\d{4}-\d{2}-\d{2}$/)
    ? req.query.startDate
    : new Date().toISOString().slice(0, 10);
  const [sy, sm, sd] = startDate.split("-").map(Number);
  const results = [];
  for (let i = 0; i < days; i++) {
    // Use a UTC-anchored Date so adding days never crosses a DST boundary
    // and produces a duplicate or skipped date.
    const d = new Date(Date.UTC(sy, sm - 1, sd));
    d.setUTCDate(d.getUTCDate() + i);
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
      // Each context makes 2 Claude calls + Spotify lookups. Cron has a 3-6 min budget,
      // so 6 contexts comfortably fits and clears the flag queue faster.
      flagResult = await processFlaggedTracks(6);
    }

    // Priority 4: rotating pool re-audit — picks the few least-recently-audited country pools
    // and re-runs auditCountryPool on each. Catches misattributions added under older logic
    // and keeps accuracy from drifting over time.
    let auditResult = { audited: 0, removed: 0 };
    try {
      auditResult = await rotateCountryPoolAudit(process.env.ANTHROPIC_API_KEY, 3);
    } catch (err) {
      console.error("[enrich] rotating audit error:", err.message);
    }

    res.json({ enrich: enrichResult, deepEnrich: deepResult, flagReview: flagResult, audit: auditResult, purged: purged ?? 0 });
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

// ── On-demand preview URL fetching ──────────────────────────────────────────
// Short-TTL in-memory cache (10 min) + in-flight deduplication.
// Collapses burst traffic (e.g. 10 users tapping same track) into 1 API call.
const PREVIEW_CACHE_TTL_MS = 10 * 60 * 1000;
const previewUrlCache = new Map(); // key → { url, cachedAt }
const previewInFlight = new Map(); // key → Promise<string|null>

function previewCacheKey({ deezerId, appleId, spotifyId }) {
  return deezerId ? `dz:${deezerId}` : appleId ? `am:${appleId}` : spotifyId ? `sp:${spotifyId}` : null;
}

async function resolvePreviewUrl({ appleId, deezerId, spotifyId, title, artist }) {
  // 1. Deezer by ID
  if (deezerId) {
    const d = await deezerEnqueue(() => deezerFetchJson(`/track/${deezerId}`, 'Deezer preview'));
    if (d?.preview) { console.log(`  [preview] Deezer by ID ${deezerId}`); return d.preview; }
  }

  // 2. Apple Music by ID
  if (appleId) {
    const appleToken = generateAppleMusicToken();
    for (const sf of APPLE_STOREFRONTS) {
      try {
        const r = await appleEnqueue(() => fetch(
          `https://api.music.apple.com/v1/catalog/${sf}/songs/${appleId}`,
          { headers: { Authorization: `Bearer ${appleToken}` } }
        ));
        if (r.ok) {
          const d = await r.json();
          const preview = d.data?.[0]?.attributes?.previews?.[0]?.url;
          if (preview) { console.log(`  [preview] Apple Music by ID ${appleId} (${sf})`); return preview; }
        }
      } catch {}
    }
  }

  // 3. Spotify by ID
  if (spotifyId) {
    try {
      const token = await getClientAccessToken();
      if (token) {
        const r = await spotifyEnqueue(() => fetch(
          `https://api.spotify.com/v1/tracks/${spotifyId}`,
          { headers: { Authorization: `Bearer ${token}` } }
        ));
        if (r.ok) {
          const d = await r.json();
          if (d.preview_url) { console.log(`  [preview] Spotify by ID ${spotifyId}`); return d.preview_url; }
        }
      }
    } catch {}
  }

  // 4. Deezer search fallback
  if (title && artist) {
    const found = await deezerTrackSearch(String(title), String(artist));
    if (found?.previewUrl) { console.log(`  [preview] Deezer search fallback for "${title}" – ${artist}`); return found.previewUrl; }
  }

  console.log(`  [preview] no preview found (appleId=${appleId}, deezerId=${deezerId}, spotifyId=${spotifyId})`);
  return null;
}

app.get("/api/preview", async (req, res) => {
  const { appleId, deezerId, spotifyId, title, artist } = req.query;

  const key = previewCacheKey({ deezerId, appleId, spotifyId });

  // Serve from short-TTL memory cache if available
  if (key) {
    const hit = previewUrlCache.get(key);
    if (hit && Date.now() - hit.cachedAt < PREVIEW_CACHE_TTL_MS) {
      return res.json({ previewUrl: hit.url });
    }
  }

  // Deduplicate concurrent requests for the same track
  if (key && previewInFlight.has(key)) {
    const url = await previewInFlight.get(key);
    return res.json({ previewUrl: url });
  }

  const promise = resolvePreviewUrl({ appleId, deezerId, spotifyId, title, artist })
    .then(url => {
      if (key) {
        previewUrlCache.set(key, { url, cachedAt: Date.now() });
        previewInFlight.delete(key);
      }
      return url;
    })
    .catch(err => {
      if (key) previewInFlight.delete(key);
      throw err;
    });

  if (key) previewInFlight.set(key, promise);

  try {
    const url = await promise;
    return res.json({ previewUrl: url });
  } catch {
    return res.json({ previewUrl: null });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎵 Musical Passport running at http://localhost:${PORT}\n`);
  // Image backfill disabled — re-enable by uncommenting when needed
  // setTimeout(() => backfillArtistImageUrls().catch(err => console.error('[image-backfill] error:', err.message)), 15_000);
});
