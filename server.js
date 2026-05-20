// Shopify UTM Orders Dashboard — FAST REST VERSION (Optimized for 1 Month)

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
   Fetch Orders Efficiently (Sequential Pagination)
===================================================== */

async function fetchOrdersPage(url){
  const r = await fetch(url,{
    headers:{ 'X-Shopify-Access-Token': TOKEN }
  });

  if (!r.ok) {
    throw new Error(await r.text());
  }

  const data = await r.json();
  const link = r.headers.get('link');

  const nextUrl = link && link.includes('rel="next"')
    ? link.match(/<([^>]+)>; rel="next"/)[1]
    : null;

  return { orders: data.orders || [], nextUrl };
}

/* =====================================================
   Preview Endpoint (Loads Only Needed Page)
===================================================== */

app.get('/api/orders', async (req, res) => {
  try {

    const { start, end, page = 1, pageSize = 50 } = req.query;

    if (!start || !end)
      return res.status(400).json({ error:'start & end required' });

    const created_at_min = new Date(start+'T00:00:00Z').toISOString();
    const created_at_max = new Date(end+'T23:59:59Z').toISOString();

    const countUrl = `https://${SHOP}/admin/api/2025-07/orders/count.json?status=any&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

    const countResp = await fetch(countUrl,{
      headers:{ 'X-Shopify-Access-Token': TOKEN }
    });

    const countData = await countResp.json();
    const shopifyTotal = countData.count || 0;

    const limit = 250;
    const targetIndexStart = (page - 1) * pageSize;
    const targetIndexEnd = targetIndexStart + Number(pageSize);

    let currentUrl = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=${limit}&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

    let collected = [];
    let currentIndex = 0;

    while (currentUrl && collected.length < targetIndexEnd){

      const { orders, nextUrl } = await fetchOrdersPage(currentUrl);

      for (const o of orders){
        if (currentIndex >= targetIndexStart && collected.length < targetIndexEnd){
          collected.push(mapOrder(o));
        }
        currentIndex++;
      }

      currentUrl = nextUrl;
    }

    res.json({
      orders: collected,
      page: Number(page),
      pageSize: Number(pageSize),
      shopify_total: shopifyTotal
    });

  } catch(err){
    res.status(500).json({ error: err.message });
  }
});

/* =====================================================
   Export CSV (Streams While Fetching)
===================================================== */
app.get('/api/export.csv', async (req, res) => {
  try {

    const { start, end } = req.query;

    if (!start || !end) {
      return res.status(400).json({ error: 'start & end required' });
    }

    const created_at_min = new Date(start + 'T00:00:00Z').toISOString();
    const created_at_max = new Date(end + 'T23:59:59Z').toISOString();

    let currentUrl =
      `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250` +
      `&created_at_min=${encodeURIComponent(created_at_min)}` +
      `&created_at_max=${encodeURIComponent(created_at_max)}`;

    // IMPORTANT HEADERS
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      'attachment; filename="shopify-utm-export.csv"'
    );
    res.setHeader('Cache-Control', 'no-cache');

    // UTF-8 BOM for Excel compatibility
    res.write('\uFEFF');

    const headers = [
      'order_number',
      'created_at',
      'utm_source',
      'utm_medium',
      'utm_campaign',
      'utm_term',
      'utm_content'
    ];

    res.write(headers.join(',') + '\n');

    const seenIds = new Set();

    while (currentUrl) {

      const r = await fetch(currentUrl, {
        headers: {
          'X-Shopify-Access-Token': TOKEN
        }
      });

      if (!r.ok) {
        throw new Error(await r.text());
      }

      const data = await r.json();

      if (!data.orders || !Array.isArray(data.orders)) {
        break;
      }

      for (const o of data.orders) {

        // DEDUPLICATION
        if (seenIds.has(o.id)) continue;
        seenIds.add(o.id);

        const row = mapOrder(o);

        // SAFE CSV ESCAPE
        const line = headers.map(key => {

          let value = row[key] || '';

          value = String(value)
            .replace(/"/g, '""')
            .replace(/\r/g, ' ')
            .replace(/\n/g, ' ');

          return `"${value}"`;

        }).join(',');

        res.write(line + '\n');
      }

      // flush chunk
      if (res.flushHeaders) {
        res.flushHeaders();
      }

      const link = r.headers.get('link');

      currentUrl =
        link && link.includes('rel="next"')
          ? link.match(/<([^>]+)>; rel="next"/)?.[1]
          : null;
    }

    res.end();

  } catch(err) {

    console.error('CSV Export Error:', err);

    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    } else {
      res.end();
    }
  }
});
/* =====================================================
   Start Server
===================================================== */

const PORT = process.env.PORT || 3000;
app.listen(PORT, ()=>console.log('Server running on', PORT));
