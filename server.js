// server.js - Shopify Bulk Operations + REST fallback
// Paste this entire file over your existing server.js (or replace the file).
// Required packages: express, node-fetch@2, cors, csv-stringify
// Ensure your package.json has these deps and you run `npm install`.

const express = require('express');
const fetch = require('node-fetch'); // v2
const cors = require('cors');
const { URL } = require('url');
const zlib = require('zlib');
const readline = require('readline');
const stream = require('stream');
const stringify = require('csv-stringify').stringify;

const app = express();
app.use(cors());
app.use(express.static('public'));

// ENV vars
const SHOP = process.env.SHOPIFY_STORE; // e.g. your-store.myshopify.com
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN; // Admin API token
const API_KEY = process.env.API_KEY || 'dev_key';

// Basic auth middleware (x-api-key)
function requireApiKey(req, res, next){
  const key = req.get('x-api-key') || req.query.api_key || '';
  if (!API_KEY || key !== API_KEY){
    return res.status(401).json({ error: 'Unauthorized - invalid API key' });
  }
  next();
}

if (!SHOP || !TOKEN) {
  console.warn('Warning: SHOP or TOKEN missing - API calls will fail until env vars are set.');
}

/* ------------------------------
   Helper: GraphQL call
   ------------------------------ */
async function shopifyGraphQL(query, variables = {}) {
  const url = `https://${SHOP}/admin/api/2025-07/graphql.json`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': TOKEN
    },
    body: JSON.stringify({ query, variables })
  });
  const json = await res.json();
  if (json.errors) throw new Error(JSON.stringify(json.errors));
  return json;
}

/* ------------------------------
   1) Start Bulk Operation
   POST /api/bulk/start
   body params (as query or JSON):
     - start (YYYY-MM-DD)
     - end   (YYYY-MM-DD)
   response: { started: true, id: 'gid://shopify/BulkOperation/123', message }
   ------------------------------ */
app.post('/api/bulk/start', requireApiKey, express.json(), async (req, res) => {
  try {
    const start = (req.body && req.body.start) || req.query.start;
    const end = (req.body && req.body.end) || req.query.end;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });

    // ISO datetimes for shopify query
    const startISO = new Date(start + 'T00:00:00Z').toISOString();
    const endISO = new Date(end + 'T23:59:59Z').toISOString();

    // Build GraphQL bulkOperation query: gather fields we care about.
    const bulkQuery = `
      mutation {
        bulkOperationRunQuery(
          query: """
          {
            orders(query: "created_at:>=${startISO} created_at:<=${endISO}", first: 250) {
              edges {
                node {
                  id
                  name
                  orderNumber: name
                  order_number: name
                  createdAt
                  landingSite
                  referringSite
                  noteAttributes { name value }
                  attributes { name value }
                }
              }
            }
          }
          """
        ) {
          bulkOperation {
            id
            status
          }
          userErrors { field message }
        }
      }
    `;

    const j = await shopifyGraphQL(bulkQuery);
    if (j.data && j.data.bulkOperationRunQuery && j.data.bulkOperationRunQuery.userErrors && j.data.bulkOperationRunQuery.userErrors.length) {
      return res.status(500).json({ error: j.data.bulkOperationRunQuery.userErrors });
    }
    const op = j.data.bulkOperationRunQuery.bulkOperation;
    return res.json({ started: true, id: op.id, status: op.status });
  } catch (err) {
    console.error('bulk/start error', err);
    res.status(500).json({ error: err.message });
  }
});

/* ------------------------------
   2) Check current bulk operation status
   GET /api/bulk/status
   response: { status, url, objectCount, id, errorCode }
   ------------------------------ */
app.get('/api/bulk/status', requireApiKey, async (req, res) => {
  try {
    const q = `{ currentBulkOperation { id status errorCode url objectCount } }`;
    const j = await shopifyGraphQL(q);
    const op = j.data.currentBulkOperation;
    res.json(op || { status: 'NONE' });
  } catch (err) {
    console.error('bulk/status error', err);
    res.status(500).json({ error: err.message });
  }
});

/* ------------------------------
   Utility: extract utm fields from urls and attributes
   ------------------------------ */
function extractUtmFromUrl(urlString){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!urlString) return out;
  try {
    const base = urlString.startsWith('http') ? urlString : 'https://' + (SHOP || 'example.com') + urlString;
    const u = new URL(base);
    for (const [k,v] of u.searchParams.entries()){
      if (k.startsWith('utm_')) out[k] = v;
    }
  } catch (e){}
  return out;
}
function extractUtmFromAttributes(attrs){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!attrs) return out;
  if (Array.isArray(attrs)){
    for (const a of attrs){
      const n = (a.name || a.key || '').toLowerCase();
      const v = a.value ? String(a.value) : '';
      if (!v) continue;
      if (n.includes('utm_source')) out.utm_source = out.utm_source || v;
      if (n.includes('utm_medium')) out.utm_medium = out.utm_medium || v;
      if (n.includes('utm_campaign')) out.utm_campaign = out.utm_campaign || v;
      if (n.includes('utm_term')) out.utm_term = out.utm_term || v;
      if (n.includes('utm_content')) out.utm_content = out.utm_content || v;
    }
  } else if (typeof attrs === 'object'){
    for (const k of Object.keys(attrs)){
      const n = k.toLowerCase();
      const v = String(attrs[k] || '');
      if (!v) continue;
      if (n.includes('utm_source')) out.utm_source = out.utm_source || v;
      if (n.includes('utm_medium')) out.utm_medium = out.utm_medium || v;
      if (n.includes('utm_campaign')) out.utm_campaign = out.utm_campaign || v;
      if (n.includes('utm_term')) out.utm_term = out.utm_term || v;
      if (n.includes('utm_content')) out.utm_content = out.utm_content || v;
    }
  }
  return out;
}
function mapOrderNodeToRow(node){
  // node fields from bulk file may be nested; adapt defensively
  const landing = node.landingSite || node.landing_site || node.landingSiteUrl || '';
  const referring = node.referringSite || node.referring_site || '';
  const landingU = extractUtmFromUrl(landing);
  const referringU = extractUtmFromUrl(referring);
  const noteU = extractUtmFromAttributes(node.noteAttributes || node.note_attributes || node.attributes || node.attrs);
  const utm = {
    utm_source: landingU.utm_source || referringU.utm_source || noteU.utm_source || '',
    utm_medium: landingU.utm_medium || referringU.utm_medium || noteU.utm_medium || '',
    utm_campaign: landingU.utm_campaign || referringU.utm_campaign || noteU.utm_campaign || '',
    utm_term: landingU.utm_term || referringU.utm_term || noteU.utm_term || '',
    utm_content: landingU.utm_content || referringU.utm_content || noteU.utm_content || ''
  };
  return {
    id: node.id || '',
    order_number: node.order_number || node.orderNumber || node.name || '',
    created_at: node.createdAt || node.created_at || '',
    ...utm
  };
}

/* ------------------------------
   3) Download / parse bulk export and return CSV
   GET /api/bulk/download
   Query params:
     - preview (optional) - if provided returns only first N rows
   Returns: CSV download (Content-Type: text/csv)
   ------------------------------ */
app.get('/api/bulk/download', requireApiKey, async (req, res) => {
  try {
    const preview = req.query.preview ? parseInt(req.query.preview, 10) : 0;
    // 1) get current bulk operation url
    const q = `{ currentBulkOperation { id status errorCode url objectCount } }`;
    const j = await shopifyGraphQL(q);
    const op = j.data.currentBulkOperation;
    if (!op || !op.status) return res.status(404).json({ error: 'No bulk operation found. Start one first.' });
    if (op.status !== 'COMPLETED') return res.status(400).json({ error: 'Bulk operation not ready. Status: ' + op.status });

    const url = op.url;
    if (!url) return res.status(500).json({ error: 'Bulk operation completed but no URL returned.' });

    // 2) download the .jsonl.gz and stream-decompress + parse lines
    const fetchRes = await fetch(url);
    if (!fetchRes.ok) throw new Error('Failed to download bulk file: ' + fetchRes.status);

    // Response body is a stream of gzipped bytes
    const gzStream = fetchRes.body;
    const gunzip = zlib.createGunzip();

    // pipe through gunzip
    const decompressed = stream.pipeline(gzStream, gunzip, (err) => {
      if (err) console.error('Pipeline error', err);
    });

    // Read line by line from the decompressed stream
    const rl = readline.createInterface({ input: gunzip });

    // Stream CSV directly to response to avoid buffering everything
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="shopify-bulk-utm.csv"');

    // Define CSV columns
    const columns = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];

    // Write CSV header
    res.write(columns.join(',') + '\\n');

    let count = 0;
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      // Each line is a JSON object representing top-level item (Shopify bulk returns objects with order fields)
      // The bulk file often contains wrapped objects; try to extract order node safely.
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (e) {
        // ignore lines that fail to parse
        continue;
      }

      // Shopify bulk usually returns objects shaped like { orders: { edges: [ { node: {...} } ] } } or plain node objects.
      // We handle both: if top-level has orders, iterate its edges; otherwise, treat parsed as node-like.
      let rows = [];
      if (parsed && parsed.orders && parsed.orders.edges) {
        for (const edge of parsed.orders.edges) {
          if (edge && edge.node) rows.push(mapOrderNodeToRow(edge.node));
        }
      } else if (parsed && parsed.node) {
        rows.push(mapOrderNodeToRow(parsed.node));
      } else {
        // sometimes the node is directly the object
        rows.push(mapOrderNodeToRow(parsed));
      }

      // Convert rows to CSV lines and write
      for (const r of rows) {
        const lineArr = columns.map(k => {
          const v = r[k] === null || r[k] === undefined ? '' : String(r[k]);
          // escape quotes
          return '"' + v.replace(/"/g, '""') + '"';
        });
        res.write(lineArr.join(',') + '\\n');
        count++;
        if (preview && count >= preview) break;
      }
      if (preview && count >= preview) break;
    }

    // finalize response
    res.end();
  } catch (err) {
    console.error('bulk/download error', err);
    res.status(500).json({ error: err.message });
  }
});

/* ------------------------------
   (Optional) Keep a small REST fallback for small ranges
   GET /api/orders?start=&end=&page=&pageSize=
   ------------------------------ */
async function fetchAllOrdersREST(created_at_min, created_at_max, maxResults = 2000){
  if (!SHOP || !TOKEN) throw new Error('Missing SHOP or TOKEN env vars');
  let url = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;
  const mapped = [];
  while (url) {
    const resp = await fetch(url, { headers: { 'X-Shopify-Access-Token': TOKEN, 'Accept':'application/json' } });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error('Shopify API error: ' + resp.status + ' - ' + text);
    }
    const data = await resp.json();
    if (data && data.orders) {
      for (const o of data.orders) {
        mapped.push({
          id: o.id,
          order_number: o.order_number || o.name || '',
          created_at: o.created_at,
          ...Object.assign({}, extractUtmFromUrl(o.landing_site), extractUtmFromAttributes(o.note_attributes || o.attributes))
        });
        if (mapped.length >= maxResults) break;
      }
    }
    if (mapped.length >= maxResults) break;
    const link = resp.headers.get('link');
    if (link && link.includes('rel="next"')) {
      const m = link.match(/<([^>]+)>; rel="next"/);
      url = m ? m[1] : null;
    } else {
      url = null;
    }
  }
  return mapped;
}

app.get('/api/orders', requireApiKey, async (req, res) => {
  try {
    const { start, end, page = '1', pageSize = '100', max } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });
    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();
    const MAX = parseInt(max || process.env.MAX_RESULTS || '2000', 10);
    const all = await fetchAllOrdersREST(created_at_min, created_at_max, MAX);
    all.sort((a,b)=> new Date(b.created_at) - new Date(a.created_at));
    const p = Math.max(1, parseInt(page,10));
    const ps = Math.max(1, Math.min(1000, parseInt(pageSize,10)));
    const startIdx = (p-1)*ps;
    const pageRows = all.slice(startIdx, startIdx + ps);
    res.json({ total: all.length, page: p, pageSize: ps, orders: pageRows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => res.sendFile(__dirname + '/public/index.html'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server listening on', PORT));
