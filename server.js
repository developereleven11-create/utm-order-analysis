// Shopify UTM Orders Dashboard — FINAL STABLE REST VERSION
// Uses REST API for accurate UTM extraction from landing_site + note_attributes

const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const { URL } = require('url');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const SHOP = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

if (!SHOP || !TOKEN) {
  console.warn('⚠️ Missing SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN');
}

/* =====================================================
   Helpers
===================================================== */

function formatDateToIST(isoString){
  if (!isoString) return '';
  const d = new Date(isoString);
  const ist = new Date(d.getTime() + 330 * 60000);
  const pad = n => String(n).padStart(2,'0');
  return `${ist.getUTCFullYear()}-${pad(ist.getUTCMonth()+1)}-${pad(ist.getUTCDate())} ${pad(ist.getUTCHours())}:${pad(ist.getUTCMinutes())}:${pad(ist.getUTCSeconds())}`;
}

function extractUtmFromUrl(urlString){
  const out = { utm_source:'', utm_medium:'', utm_campaign:'', utm_term:'', utm_content:'' };
  if (!urlString) return out;

  try {
    const u = new URL(urlString.startsWith('http') ? urlString : 'https://dummy.com'+urlString);
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

function mapOrder(o){
  const utmUrl = extractUtmFromUrl(o.landing_site);
  const utmAttr = extractUtmFromAttributes(o.note_attributes || []);

  return {
    order_number: o.name,
    created_at: formatDateToIST(o.created_at),
    utm_source: utmUrl.utm_source || utmAttr.utm_source || '',
    utm_medium: utmUrl.utm_medium || utmAttr.utm_medium || '',
    utm_campaign: utmUrl.utm_campaign || utmAttr.utm_campaign || '',
    utm_term: utmUrl.utm_term || utmAttr.utm_term || '',
    utm_content: utmUrl.utm_content || utmAttr.utm_content || ''
  };
}

/* =====================================================
   Fetch Orders Properly (No Duplicates)
===================================================== */

async function fetchAllOrders(created_at_min, created_at_max){

  let url = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

  const allOrders = [];
  const seenIds = new Set();

  while (url){

    const r = await fetch(url,{
      headers:{ 'X-Shopify-Access-Token': TOKEN }
    });

    if (!r.ok) {
      console.error("Shopify error:", await r.text());
      break;
    }

    const data = await r.json();

    if (!data.orders || !Array.isArray(data.orders)) break;

    for (const o of data.orders){
      if (seenIds.has(o.id)) continue;
      seenIds.add(o.id);
      allOrders.push(o);
    }

    const link = r.headers.get('link');
    url = link && link.includes('rel="next"')
      ? link.match(/<([^>]+)>; rel="next"/)[1]
      : null;

    // small delay to avoid rate limit
    await new Promise(resolve => setTimeout(resolve, 150));
  }

  return allOrders;
}

/* =====================================================
   Preview Endpoint
===================================================== */

app.get('/api/orders', async (req, res) => {
  try {

    const { start, end, page = 1, pageSize = 50 } = req.query;

    if (!start || !end)
      return res.status(400).json({ error:'start & end required' });

    const created_at_min = new Date(start+'T00:00:00Z').toISOString();
    const created_at_max = new Date(end+'T23:59:59Z').toISOString();

    // Get total count
    const countUrl = `https://${SHOP}/admin/api/2025-07/orders/count.json?status=any&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

    const countResp = await fetch(countUrl,{
      headers:{ 'X-Shopify-Access-Token': TOKEN }
    });

    const countData = await countResp.json();
    const shopifyTotal = countData.count || 0;

    const ordersRaw = await fetchAllOrders(created_at_min, created_at_max);

    // Sort by created_at descending
    ordersRaw.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));

    const mapped = ordersRaw.map(mapOrder);

    const p = Number(page);
    const ps = Number(pageSize);
    const paged = mapped.slice((p-1)*ps, (p-1)*ps+ps);

    res.json({
      orders: paged,
      page: p,
      pageSize: ps,
      total_fetched: mapped.length,
      shopify_total: shopifyTotal
    });

  } catch(err){
    res.status(500).json({ error: err.message });
  }
});

/* =====================================================
   Export CSV
===================================================== */

app.get('/api/export.csv', async (req, res) => {
  try {

    const { start, end } = req.query;

    if (!start || !end)
      return res.status(400).json({ error:'start & end required' });

    const created_at_min = new Date(start+'T00:00:00Z').toISOString();
    const created_at_max = new Date(end+'T23:59:59Z').toISOString();

    const ordersRaw = await fetchAllOrders(created_at_min, created_at_max);
    ordersRaw.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));

    const mapped = ordersRaw.map(mapOrder);

    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition','attachment; filename="shopify-utm-export.csv"');

    const headers = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.write(headers.join(',') + '\n');

    for(const row of mapped){
      const line = headers.map(k =>
        `"${(row[k]||'').replace(/"/g,'""')}"`
      ).join(',');
      res.write(line + '\n');
    }

    res.end();

  } catch(err){
    res.status(500).json({ error: err.message });
  }
});

/* =====================================================
   Start Server
===================================================== */

const PORT = process.env.PORT || 3000;
app.listen(PORT, ()=>console.log('Server running on', PORT));
