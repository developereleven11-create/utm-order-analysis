// server.js — Shopify UTM Orders Dashboard (Stable Bulk Version)

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
app.use(express.static('public'));

// =====================
// ENV CONFIG
// =====================

const SHOP = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

const API_KEY = process.env.API_KEY || 'dev_key';
const ENFORCE = String(process.env.ENFORCE_API_KEY || '').trim() === '1';

if (!SHOP || !TOKEN) {
  console.warn('⚠️ Missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN');
}

// Optional API key enforcement
function requireApiKey(req, res, next) {
  if (!ENFORCE) return next();
  const key = req.get('x-api-key') || req.query.api_key || '';
  if (key !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized - invalid API key' });
  }
  next();
}

// =====================
// SHOPIFY GRAPHQL HELPER
// =====================

async function shopifyGraphQL(query) {
  const url = `https://${SHOP}/admin/api/2025-07/graphql.json`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': TOKEN
    },
    body: JSON.stringify({ query })
  });

  const json = await res.json();
  if (json.errors) throw new Error(JSON.stringify(json.errors));
  return json;
}

// =====================
// DATE FORMAT (IST)
// =====================

function formatDateToIST(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  const istMs = d.getTime() + (330 * 60 * 1000);
  const ist = new Date(istMs);
  const pad = (n) => String(n).padStart(2, '0');
  return `${ist.getUTCFullYear()}-${pad(ist.getUTCMonth()+1)}-${pad(ist.getUTCDate())} ${pad(ist.getUTCHours())}:${pad(ist.getUTCMinutes())}:${pad(ist.getUTCSeconds())}`;
}

// =====================
// UTM EXTRACTION
// =====================

function extractUtmFromUrl(urlString){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!urlString) return out;
  try {
    const base = urlString.startsWith('http') ? urlString : 'https://dummy.com' + urlString;
    const u = new URL(base);
    for (const [k,v] of u.searchParams.entries()){
      if (k.startsWith('utm_')) out[k] = v;
    }
  } catch {}
  return out;
}

function extractUtmFromAttributes(attrs){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!Array.isArray(attrs)) return out;

  for (const a of attrs){
    const n = (a.name || '').toLowerCase();
    const v = a.value || '';
    if (!v) continue;
    if (n.includes('utm_source')) out.utm_source = v;
    if (n.includes('utm_medium')) out.utm_medium = v;
    if (n.includes('utm_campaign')) out.utm_campaign = v;
    if (n.includes('utm_term')) out.utm_term = v;
    if (n.includes('utm_content')) out.utm_content = v;
  }
  return out;
}

function mapOrder(node){
  const utmUrl = extractUtmFromUrl(node.landingSite || node.referringSite || '');
  const utmAttr = extractUtmFromAttributes(node.noteAttributes || []);
  return {
    order_number: node.name || '',
    created_at: formatDateToIST(node.createdAt),
    utm_source: utmUrl.utm_source || utmAttr.utm_source || '',
    utm_medium: utmUrl.utm_medium || utmAttr.utm_medium || '',
    utm_campaign: utmUrl.utm_campaign || utmAttr.utm_campaign || '',
    utm_term: utmUrl.utm_term || utmAttr.utm_term || '',
    utm_content: utmUrl.utm_content || utmAttr.utm_content || ''
  };
}

// =====================
// BULK START (FIXED)
// =====================

app.post('/api/bulk/start', requireApiKey, async (req, res) => {
  try {
    const { start, end } = req.body;
    if (!start || !end) {
      return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });
    }

    const startISO = new Date(start + 'T00:00:00Z').toISOString();
    const endISO = new Date(end + 'T23:59:59Z').toISOString();

    const query = `
      mutation {
        bulkOperationRunQuery(
          query: """
          {
            orders(query: "created_at:>=${startISO} created_at:<=${endISO}") {
              id
              name
              createdAt
              landingSite
              referringSite
              noteAttributes {
                name
                value
              }
            }
          }
          """
        ) {
          bulkOperation { id status }
          userErrors { field message }
        }
      }
    `;

    const response = await shopifyGraphQL(query);
    const result = response.data.bulkOperationRunQuery;

    if (result.userErrors.length > 0) {
      return res.status(400).json({
        error: "Shopify userErrors",
        userErrors: result.userErrors
      });
    }

    res.json({ started: true, id: result.bulkOperation.id, status: result.bulkOperation.status });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =====================
// BULK STATUS
// =====================

app.get('/api/bulk/status', requireApiKey, async (req, res) => {
  try {
    const query = `{ currentBulkOperation { id status errorCode objectCount url } }`;
    const response = await shopifyGraphQL(query);
    res.json(response.data.currentBulkOperation || { status: 'NONE' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =====================
// BULK DOWNLOAD
// =====================

app.get('/api/bulk/download', requireApiKey, async (req, res) => {
  try {
    const statusQuery = `{ currentBulkOperation { status url } }`;
    const statusResp = await shopifyGraphQL(statusQuery);
    const op = statusResp.data.currentBulkOperation;

    if (!op || op.status !== 'COMPLETED') {
      return res.status(400).json({ error: 'Bulk not completed yet' });
    }

    const fetchRes = await fetch(op.url);
    const gunzip = zlib.createGunzip();
    stream.pipeline(fetchRes.body, gunzip, () => {});

    const rl = readline.createInterface({ input: gunzip });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="shopify-bulk-utm.csv"');

    const columns = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.write(columns.join(',') + '\n');

    for await (const line of rl) {
      if (!line.trim()) continue;
      const parsed = JSON.parse(line);
      const row = mapOrder(parsed);
      const csv = columns.map(k => `"${(row[k]||'').replace(/"/g,'""')}"`).join(',');
      res.write(csv + '\n');
    }

    res.end();

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =====================
// SERVER START
// =====================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server running on port', PORT));
