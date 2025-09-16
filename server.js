const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const stringify = require('csv-stringify').stringify;
const { URL } = require('url');

const app = express();
app.use(cors());
app.use(express.static('public'));

const SHOP = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API_KEY = process.env.API_KEY || 'dev_key';
if (!SHOP || !TOKEN) {
  console.warn('Warning: SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN not set. API routes will fail without them.');
}

function extractUtmFromUrl(urlString){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!urlString) return out;
  try {
    const base = urlString.startsWith('http') ? urlString : 'https://' + (process.env.SHOPIFY_STORE || 'example.com') + urlString;
    const u = new URL(base);
    for (const [k,v] of u.searchParams.entries()){
      if (k.startsWith('utm_')) out[k] = v;
    }
  } catch (e) {}
  return out;
}

// Extract from note_attributes array or attributes object-like array
function extractUtmFromAttributes(attrs){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!attrs) return out;
  // note_attributes is [{name, value}, ...]
  if (Array.isArray(attrs)){
    for (const a of attrs){
      const n = (a.name || a.key || '').toLowerCase();
      const v = (a.value || a.value === 0) ? String(a.value) : '';
      if (!v) continue;
      if (n.includes('utm_source')) out.utm_source = out.utm_source || v;
      if (n.includes('utm_medium')) out.utm_medium = out.utm_medium || v;
      if (n.includes('utm_campaign')) out.utm_campaign = out.utm_campaign || v;
      if (n.includes('utm_term')) out.utm_term = out.utm_term || v;
      if (n.includes('utm_content')) out.utm_content = out.utm_content || v;
    }
  } else if (typeof attrs === 'object'){
    // e.g. attributes as key-value map
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

async function fetchAllOrders(created_at_min, created_at_max){
  if (!SHOP || !TOKEN) throw new Error('Missing SHOP or TOKEN env vars');
  let url = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;
  const orders = [];
  while (url){
    const res = await fetch(url, {
      headers: {'X-Shopify-Access-Token': TOKEN, 'Accept':'application/json'}
    });
    if (!res.ok){
      const text = await res.text();
      throw new Error('Shopify API error: ' + res.status + ' - ' + text);
    }
    const data = await res.json();
    if (data && data.orders) orders.push(...data.orders);
    const link = res.headers.get('link');
    if (link && link.includes('rel="next"')){
      const m = link.match(/<([^>]+)>; rel="next"/);
      url = m ? m[1] : null;
    } else {
      url = null;
    }
  }
  return orders;
}

function mapOrderToRow(o){
  const landingUtm = extractUtmFromUrl(o.landing_site);
  const referringUtm = extractUtmFromUrl(o.referring_site);
  const noteUtm = extractUtmFromAttributes(o.note_attributes || o.attributes);
  const utm = {
    utm_source: landingUtm.utm_source || referringUtm.utm_source || noteUtm.utm_source || '',
    utm_medium: landingUtm.utm_medium || referringUtm.utm_medium || noteUtm.utm_medium || '',
    utm_campaign: landingUtm.utm_campaign || referringUtm.utm_campaign || noteUtm.utm_campaign || '',
    utm_term: landingUtm.utm_term || referringUtm.utm_term || noteUtm.utm_term || '',
    utm_content: landingUtm.utm_content || referringUtm.utm_content || noteUtm.utm_content || ''
  };
  return {
    id: o.id,
    order_number: o.order_number || o.name || '',
    created_at: o.created_at,
    ...utm
  };
}

// simple auth middleware
function requireApiKey(req, res, next){
  const key = req.get('x-api-key') || req.query.api_key || '';
  if (!API_KEY || key !== API_KEY){
    return res.status(401).json({ error: 'Unauthorized - invalid API key' });
  }
  next();
}

// Returns paginated JSON
app.get('/api/orders', requireApiKey, async (req, res) => {
  try {
    const { start, end, page = '1', pageSize = '100' } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });
    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();
    const rawOrders = await fetchAllOrders(created_at_min, created_at_max);
    const mapped = rawOrders.map(mapOrderToRow);
    // sort desc by created_at
    mapped.sort((a,b)=> new Date(b.created_at) - new Date(a.created_at));
    const p = Math.max(1, parseInt(page,10));
    const ps = Math.max(1, Math.min(1000, parseInt(pageSize,10)));
    const startIdx = (p-1)*ps;
    const pageRows = mapped.slice(startIdx, startIdx + ps);
    res.json({ total: mapped.length, page: p, pageSize: ps, orders: pageRows });
  } catch (err){
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// CSV export endpoint - returns CSV for selected columns
app.get('/api/export.csv', requireApiKey, async (req, res) => {
  try {
    const { start, end, columns } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required' });
    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();
    const rawOrders = await fetchAllOrders(created_at_min, created_at_max);
    const mapped = rawOrders.map(mapOrderToRow);
    const cols = (columns || 'order_number,created_at,utm_source,utm_medium,utm_campaign,utm_term,utm_content').split(',');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="shopify-utm-orders.csv"');
    stringify(mapped, { header: true, columns: cols }, (err, output) => {
      if (err) return res.status(500).send('CSV generation error');
      res.send(output);
    });
  } catch (err){
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// serve index
app.get('/', (req, res) => res.sendFile(__dirname + '/public/index.html'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Server listening on', PORT));
