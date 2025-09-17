// server.js - Shopify UTM Orders Dashboard
// Default: API endpoints are unrestricted (no API key required).
// Toggle enforcement by setting ENFORCE_API_KEY=1 and API_KEY=<your_key> in env.
//
// Required packages: express, node-fetch@2, cors
// Ensure package.json has these deps and run `npm install`.

const express = require('express');
const fetch = require('node-fetch'); // v2
const cors = require('cors');
const { URL } = require('url');
const zlib = require('zlib');
const readline = require('readline');
const stream = require('stream');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public')); // serve UI from public/

// ENV vars
const SHOP = process.env.SHOPIFY_STORE; // e.g. your-store.myshopify.com
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN; // Admin API token
const API_KEY = process.env.API_KEY || 'dev_key'; // used only when ENFORCE_API_KEY=1
const ENFORCE = String(process.env.ENFORCE_API_KEY || '').trim() === '1';

if (!SHOP || !TOKEN) {
  console.warn('Warning: SHOP or TOKEN missing - API calls will fail until env vars are set.');
}

// Middleware: requireApiKey only when ENFORCE_API_KEY=1
function requireApiKey(req, res, next){
  if (!ENFORCE) {
    // Enforcement disabled — allow all requests through.
    return next();
  }
  const key = req.get('x-api-key') || req.query.api_key || '';
  if (!API_KEY || key !== API_KEY){
    return res.status(401).json({ error: 'Unauthorized - invalid API key' });
  }
  next();
}

/* ------------------------------
   Helpers: GraphQL + UTM parsers + date format
   ------------------------------ */
async function shopifyGraphQL(query, variables = {}) {
  if (!SHOP || !TOKEN) throw new Error('Missing SHOP or TOKEN env vars');
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

// Format ISO date into IST 'YYYY-MM-DD HH:mm:ss'
function formatDateToIST(isoString){
  if (!isoString) return '';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return '';
  // IST = UTC + 5.5 hours = 330 minutes
  const istMs = d.getTime() + (330 * 60 * 1000);
  const ist = new Date(istMs);
  const pad = (n) => String(n).padStart(2, '0');
  const YYYY = ist.getUTCFullYear();
  const MM = pad(ist.getUTCMonth() + 1);
  const DD = pad(ist.getUTCDate());
  const hh = pad(ist.getUTCHours());
  const mm = pad(ist.getUTCMinutes());
  const ss = pad(ist.getUTCSeconds());
  return `${YYYY}-${MM}-${DD} ${hh}:${mm}:${ss}`;
}

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
  const landing = node.landingSite || node.landing_site || '';
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

  const rawName = (node.name || node.order_number || node.orderNumber || '') + '';
  const createdAtRaw = node.createdAt || node.created_at || '';
  return {
    id: node.id || '',
    order_number: rawName,
    created_at: formatDateToIST(createdAtRaw),
    created_at_raw: createdAtRaw,
    ...utm
  };
}

/* ------------------------------
   REST fallback: fetch orders via REST (maps to IST + full order name)
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
        const rawName = (o.name || o.order_number || '') + '';
        const createdAtRaw = o.created_at || '';
        mapped.push({
          id: o.id,
          order_number: rawName,
          created_at: formatDateToIST(createdAtRaw),
          created_at_raw: createdAtRaw,
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

// Helper: count orders for a date range using Shopify count endpoint
async function fetchOrdersCount(created_at_min, created_at_max){
  if (!SHOP || !TOKEN) throw new Error('Missing SHOP or TOKEN env vars');
  const countUrl = `https://${SHOP}/admin/api/2025-07/orders/count.json?status=any&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;
  const r = await fetch(countUrl, { headers: { 'X-Shopify-Access-Token': TOKEN, 'Accept': 'application/json' } });
  if (!r.ok) {
    const t = await r.text();
    throw new Error('Shopify count error: ' + r.status + ' - ' + t);
  }
  const j = await r.json();
  return j.count || 0;
}

/* ------------------------------
   Routes
   - Bulk start/status/download
   - /api/orders
   - /api/export.csv
   All routes call requireApiKey which is a no-op unless ENFORCE_API_KEY=1
   ------------------------------ */

/* Start bulk */
app.post('/api/bulk/start', requireApiKey, async (req, res) => {
  try {
    const start = (req.body && req.body.start) || req.query.start;
    const end = (req.body && req.body.end) || req.query.end;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });

    const startISO = new Date(start + 'T00:00:00Z').toISOString();
    const endISO = new Date(end + 'T23:59:59Z').toISOString();

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

/* Bulk status */
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

/* Bulk download */
app.get('/api/bulk/download', requireApiKey, async (req, res) => {
  try {
    const preview = req.query.preview ? parseInt(req.query.preview, 10) : 0;
    const q = `{ currentBulkOperation { id status errorCode url objectCount } }`;
    const j = await shopifyGraphQL(q);
    const op = j.data.currentBulkOperation;
    if (!op || !op.status) return res.status(404).json({ error: 'No bulk operation found. Start one first.' });
    if (op.status !== 'COMPLETED') return res.status(400).json({ error: 'Bulk operation not ready. Status: ' + op.status });

    const url = op.url;
    if (!url) return res.status(500).json({ error: 'Bulk operation completed but no URL returned.' });

    const fetchRes = await fetch(url);
    if (!fetchRes.ok) throw new Error('Failed to download bulk file: ' + fetchRes.status);

    const gzStream = fetchRes.body;
    const gunzip = zlib.createGunzip();
    stream.pipeline(gzStream, gunzip, (err) => { if (err) console.error('Pipeline error', err); });

    const rl = readline.createInterface({ input: gunzip });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="shopify-bulk-utm.csv"');

    const columns = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.write(columns.join(',') + '\n');

    let count = 0;
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let parsed;
      try { parsed = JSON.parse(line); } catch (e) { continue; }

      let rows = [];
      if (parsed && parsed.orders && parsed.orders.edges) {
        for (const edge of parsed.orders.edges) {
          if (edge && edge.node) rows.push(mapOrderNodeToRow(edge.node));
        }
      } else if (parsed && parsed.node) {
        rows.push(mapOrderNodeToRow(parsed.node));
      } else {
        rows.push(mapOrderNodeToRow(parsed));
      }

      for (const r of rows) {
        const lineArr = columns.map(k => {
          const v = r[k] === null || r[k] === undefined ? '' : String(r[k]);
          return '"' + v.replace(/"/g, '""') + '"';
        });
        res.write(lineArr.join(',') + '\n');
        count++;
        if (preview && count >= preview) break;
      }
      if (preview && count >= preview) break;
    }
    res.end();
  } catch (err) {
    console.error('bulk/download error', err);
    res.status(500).json({ error: err.message });
  }
});

/* REST orders endpoint */
app.get('/api/orders', requireApiKey, async (req, res) => {
  try {
    const { start, end, page = '1', pageSize = '100', max } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });

    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();

    const MAX = parseInt(max || process.env.MAX_RESULTS || '2000', 10);
    const createdRows = await fetchAllOrdersREST(created_at_min, created_at_max, MAX);

    let shopifyTotal = null;
    try {
      shopifyTotal = await fetchOrdersCount(created_at_min, created_at_max);
    } catch (e) {
      console.warn('Could not fetch shopify count:', e.message);
      shopifyTotal = null;
    }

    createdRows.sort((a,b) => new Date(b.created_at_raw || b.created_at) - new Date(a.created_at_raw || a.created_at));
    const p = Math.max(1, parseInt(page,10));
    const ps = Math.max(1, Math.min(1000, parseInt(pageSize,10)));
    const startIdx = (p-1)*ps;
    const pageRows = createdRows.slice(startIdx, startIdx + ps);

    res.json({
      total_fetched: createdRows.length,
      page: p,
      pageSize: ps,
      orders: pageRows,
      shopify_total: shopifyTotal
    });
  } catch (err) {
    console.error('/api/orders error', err);
    res.status(500).json({ error: err.message });
  }
});

/* Export CSV endpoint */
app.get('/api/export.csv', requireApiKey, async (req, res) => {
  try {
    const { start, end, useBulk = '0', preview = '0' } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });

    if (useBulk === '1') {
      const q = `{ currentBulkOperation { id status errorCode url objectCount } }`;
      const statusResp = await shopifyGraphQL(q);
      const op = statusResp.data.currentBulkOperation;
      if (!op) return res.status(400).json({ error: 'No bulk operation found. Start one with /api/bulk/start' });
      if (op.status !== 'COMPLETED') return res.status(400).json({ error: 'Bulk not ready. Status: ' + op.status });
      return res.redirect(303, '/api/bulk/download?preview=' + encodeURIComponent(preview || '0'));
    }

    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();
    const MAX = parseInt(process.env.MAX_RESULTS || '5000', 10);
    const rows = await fetchAllOrdersREST(created_at_min, created_at_max, MAX);

    const columns = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="shopify-utm-export.csv"');
    res.write(columns.join(',') + '\n');

    let count = 0;
    for (const r of rows) {
      const line = columns.map(k => {
        const v = r[k] === null || r[k] === undefined ? '' : String(r[k]);
        return '"' + v.replace(/"/g, '""') + '"';
      }).join(',');
      res.write(line + '\n');
      count++;
      if (parseInt(preview, 10) && count >= parseInt(preview, 10)) break;
    }
    res.end();
  } catch (err) {
    console.error('/api/export.csv error', err);
    res.status(500).json({ error: err.message });
  }
});

// serve index.html root
app.get('/', (req, res) => res.sendFile(__dirname + '/public/index.html'));

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server listening on', PORT));
