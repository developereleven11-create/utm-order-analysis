# Shopify UTM Orders Dashboard — v2 (Enhanced)

What this repo includes
- Improved Express server (`server.js`) with:
  - Authenticated API using `X-API-KEY` header (set `API_KEY` env var).
  - Fetches orders from Shopify Admin REST API (paginated via Link header).
  - Extracts UTM params from `landing_site`, `referring_site`, `note_attributes`, and `attributes`.
  - Server-side pagination support and CSV export endpoint.
- Frontend (`/public/index.html`) — React (UMD) + small UI:
  - Date range picker, fetch with API key, pagination controls.
  - Column selector for CSV export.
- `package.json`, `.env.example`, `vercel.json`.

Required Shopify access/scopes
- `read_orders`

Environment variables (set in Vercel or .env locally)
- SHOPIFY_STORE=your-store.myshopify.com
- SHOPIFY_ACCESS_TOKEN=shpat_xxx
- API_KEY=a_random_string_used_to_protect_api_calls

Run locally
1. npm install
2. create .env with the above
3. npm start
4. Open http://localhost:3000

Notes
- The server fetches all orders in the date range (Shopify limit 250 per request) and then paginates results server-side.
- For large stores / wide date ranges consider adding cursor-based queries or background jobs.
- This version improves UTM extraction by also checking `note_attributes` and `attributes`. If you track UTMs in a different place, we can add it.
