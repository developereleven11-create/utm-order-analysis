// Shopify UTM Orders Dashboard — FULL PRODUCTION VERSION

const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const { URL } = require('url');
const zlib = require('zlib');
const readline = require('readline');
const stream = require('stream');

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
   HELPERS
===================================================== */

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
    const n = (a.name||'').toLowerCase();
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

function mapOrderFromREST(o){
  const utmUrl = extractUtmFromUrl(o.landing_site);
  const utmAttr = extractUtmFromAttributes(o.note_attributes||[]);
  return {
    order_number:o.name,
    created_at:formatDateToIST(o.created_at),
    utm_source:utmUrl.utm_source||utmAttr.utm_source||'',
    utm_medium:utmUrl.utm_medium||utmAttr.utm_medium||'',
    utm_campaign:utmUrl.utm_campaign||utmAttr.utm_campaign||'',
    utm_term:utmUrl.utm_term||utmAttr.utm_term||'',
    utm_content:utmUrl.utm_content||utmAttr.utm_content||''
  };
}

function mapOrderFromBulk(node){
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

/* =====================================================
   REST PREVIEW (SAFE + PAGINATED)
===================================================== */

app.get('/api/orders', async (req, res) => {
  try {
    const { start, end, page = 1, pageSize = 50 } = req.query;
    if (!start || !end) return res.status(400).json({ error:'start & end required' });

    const created_at_min = new Date(start+'T00:00:00Z').toISOString();
    const created_at_max = new Date(end+'T23:59:59Z').toISOString();

    // Shopify total count
    let shopifyTotal = 0;
    try {
      const countUrl = `https://${SHOP}/admin/api/2025-07/orders/count.json?status=any&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;
      const countResp = await fetch(countUrl, { headers:{'X-Shopify-Access-Token':TOKEN}});
      const countData = await countResp.json();
      shopifyTotal = countData.count || 0;
    } catch(e){
      console.warn("Count fetch failed:", e.message);
    }

    let url = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

    const allRows = [];
    const MAX_SAFE_FETCH = 5000;

    while(url && allRows.length < MAX_SAFE_FETCH){

      const r = await fetch(url,{ headers:{'X-Shopify-Access-Token':TOKEN}});
      if (!r.ok) break;

      const data = await r.json();
      if (!data.orders || !Array.isArray(data.orders)) break;

      for(const o of data.orders){
        allRows.push(mapOrderFromREST(o));
      }

      const link = r.headers.get('link');
      url = link && link.includes('rel="next"') ? link.match(/<([^>]+)>/)[1] : null;

      await new Promise(resolve => setTimeout(resolve, 200));
    }

    const p = Number(page);
    const ps = Number(pageSize);
    const paged = allRows.slice((p-1)*ps,(p-1)*ps+ps);

    res.json({
      orders:paged,
      page:p,
      pageSize:ps,
      total_fetched:allRows.length,
      shopify_total:shopifyTotal
    });

  } catch(err){
    res.status(500).json({ error:err.message });
  }
});

/* =====================================================
   EXPORT CSV (REST)
===================================================== */

app.get('/api/export.csv', async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end)
      return res.status(400).json({ error: 'start & end required' });

    const created_at_min = new Date(start+'T00:00:00Z').toISOString();
    const created_at_max = new Date(end+'T23:59:59Z').toISOString();

    let url = `https://${SHOP}/admin/api/2025-07/orders.json?status=any&limit=250&created_at_min=${encodeURIComponent(created_at_min)}&created_at_max=${encodeURIComponent(created_at_max)}`;

    const rows = [];
    const MAX_SAFE_EXPORT = 10000;

    while(url && rows.length < MAX_SAFE_EXPORT){

      const r = await fetch(url,{ headers:{'X-Shopify-Access-Token':TOKEN}});
      if (!r.ok) break;

      const data = await r.json();
      if (!data.orders || !Array.isArray(data.orders)) break;

      for(const o of data.orders){
        rows.push(mapOrderFromREST(o));
      }

      const link = r.headers.get('link');
      url = link && link.includes('rel="next"') ? link.match(/<([^>]+)>/)[1] : null;

      await new Promise(resolve => setTimeout(resolve, 200));
    }

    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition','attachment; filename="shopify-export.csv"');

    const headers = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.write(headers.join(',') + '\n');

    for(const row of rows){
      const line = headers.map(k => `"${(row[k]||'').replace(/"/g,'""')}"`).join(',');
      res.write(line + '\n');
    }

    res.end();

  } catch(err){
    res.status(500).json({ error: err.message });
  }
});

/* =====================================================
   BULK START
===================================================== */

app.post('/api/bulk/start', async (req,res)=>{
  try{
    const { start, end } = req.body;
    if (!start || !end) return res.status(400).json({error:'start & end required'});

    const startISO = new Date(start+'T00:00:00Z').toISOString();
    const endISO = new Date(end+'T23:59:59Z').toISOString();

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
              noteAttributes { name value }
            }
          }
          """
        ){
          bulkOperation { id status }
          userErrors { field message }
        }
      }
    `;

    const r = await shopifyGraphQL(query);

    if(r.data.bulkOperationRunQuery.userErrors.length){
      return res.status(400).json(r.data.bulkOperationRunQuery.userErrors);
    }

    res.json(r.data.bulkOperationRunQuery.bulkOperation);

  }catch(err){ 
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   BULK STATUS
===================================================== */

app.get('/api/bulk/status', async (req,res)=>{
  try{
    const q=`{ currentBulkOperation { status url objectCount } }`;
    const r=await shopifyGraphQL(q);
    res.json(r.data.currentBulkOperation||{status:'NONE'});
  }catch(err){
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   BULK DOWNLOAD
===================================================== */

app.get('/api/bulk/download', async (req,res)=>{
  try{
    const q=`{ currentBulkOperation { status url } }`;
    const r=await shopifyGraphQL(q);
    const op=r.data.currentBulkOperation;

    if(!op||op.status!=='COMPLETED')
      return res.status(400).json({error:'Bulk not ready'});

    const fetchRes=await fetch(op.url);
    const gunzip=zlib.createGunzip();
    stream.pipeline(fetchRes.body,gunzip,()=>{});
    const rl=readline.createInterface({input:gunzip});

    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition','attachment; filename="bulk.csv"');
    res.write('order_number,created_at,utm_source,utm_medium,utm_campaign,utm_term,utm_content\n');

    for await (const line of rl){
      if(!line.trim()) continue;
      const parsed=JSON.parse(line);
      const row=mapOrderFromBulk(parsed);
      const csv=Object.values(row).map(v=>`"${(v||'').replace(/"/g,'""')}"`).join(',');
      res.write(csv+'\n');
    }

    res.end();

  }catch(err){
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   SERVER START
===================================================== */

const PORT=process.env.PORT||3000;
app.listen(PORT,()=>console.log('Server running on',PORT));
