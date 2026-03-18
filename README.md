# 🎵 Musical Passport

AI-powered world music discovery. Click any country and get personalized artist recommendations with cultural context, era tags, and direct Spotify links — powered by Claude.

## Setup

### 1. Get an Anthropic API key
Sign up at [console.anthropic.com](https://console.anthropic.com) and create an API key.

### 2. Install dependencies
```bash
npm install
```

### 3. Add your API key
Copy the example env file and fill in your key:
```bash
cp .env.example .env
```
Then edit `.env`:
```
ANTHROPIC_API_KEY=sk-ant-...
```

### 4. Run the app
```bash
npm start
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Development mode (auto-restart on changes)
```bash
npm run dev
```

## Project structure

```
musical-passport/
├── public/
│   └── index.html      # Full frontend UI
├── server.js           # Express server + Anthropic API proxy
├── package.json
├── .env                # Your API key (never commit this)
└── .env.example        # Template
```

## How it works

- The **frontend** (`public/index.html`) is a single-page app with country buttons organized by region.
- Clicking a country calls the local `/api/recommend` endpoint in `server.js`.
- The **server** forwards the request to the Anthropic API using your key and returns structured JSON.
- The frontend renders artist cards with genre tags, era badges, and Spotify search links.
- Countries turn gold once "stamped" (visited).

## Next steps to build on this

- **Spotify OAuth**: Authenticate users and read their top artists to personalize recommendations
- **User accounts**: Persist passport stamps across sessions (add Supabase or SQLite)
- **Shareable passport**: Generate a visual "passport page" image users can share
- **World map**: Swap the country list for a clickable SVG/D3 map
- **"Sounds like" filtering**: Let users input a favorite artist and highlight matching countries

## License
MIT
