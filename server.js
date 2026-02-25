// Shopify UTM Orders Dashboard — FINAL STABLE GRAPHQL VERSION

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
   GraphQL Helper
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

  if (json.errors) {
    console.error('GraphQL error:', json.errors);
    throw new Error(JSON.stringify(json.errors));
  }

  return json;
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
    const u = new URL(urlString.startsWith('http') ? urlString : 'https://dummy.com' + urlString);
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
    order_number: node.name,
    created_at: formatDateToIST(node.createdAt),
    utm_source: utmUrl.utm_source || utmAttr.utm_source || '',
    utm_medium: utmUrl.utm_medium || utmAttr.utm_medium || '',
    utm_campaign: utmUrl.utm_campaign || utmAttr.utm_campaign || '',
    utm_term: utmUrl.utm_term || utmAttr.utm_term || '',
    utm_content: utmUrl.utm_content || utmAttr.utm_content || ''
  };
}

/* =====================================================
   PREVIEW (GraphQL Paginated)
===================================================== */

app.get('/api/orders', async (req, res) => {
  try {
    const { start, end, cursor } = req.query;

    if (!start || !end)
      return res.status(400).json({ error: 'start & end required' });

    const startISO = new Date(start+'T00:00:00Z').toISOString();
    const endISO = new Date(end+'T23:59:59Z').toISOString();

    const query = `
      {
        orders(
          first: 100,
          ${cursor ? `after: "${cursor}",` : ""}
          query: "status:any created_at:>=${startISO} created_at:<=${endISO}"
        ) {
          edges {
            cursor
            node {
              id
              name
              createdAt
              landingSite
              referringSite
              noteAttributes { name value }
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    `;

    const r = await shopifyGraphQL(query);

    const edges = r.data.orders.edges;

    const rows = edges.map(e => mapOrder(e.node));

    res.json({
      orders: rows,
      nextCursor: edges.length ? edges[edges.length - 1].cursor : null,
      hasNextPage: r.data.orders.pageInfo.hasNextPage
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* =====================================================
   BULK START
===================================================== */

app.post('/api/bulk/start', async (req,res)=>{
  try{
    const { start, end } = req.body;

    if (!start || !end)
      return res.status(400).json({error:'start & end required'});

    const startISO = new Date(start+'T00:00:00Z').toISOString();
    const endISO = new Date(end+'T23:59:59Z').toISOString();

    const query = `
      mutation {
        bulkOperationRunQuery(
          query: """
          {
            orders(query: "status:any created_at:>=${startISO} created_at:<=${endISO}") {
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

    if (r.data.bulkOperationRunQuery.userErrors.length) {
      return res.status(400).json(r.data.bulkOperationRunQuery.userErrors);
    }

    res.json(r.data.bulkOperationRunQuery.bulkOperation);

  } catch(err){
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   BULK STATUS
===================================================== */

app.get('/api/bulk/status', async (req,res)=>{
  try{
    const q = `{ currentBulkOperation { status url objectCount } }`;
    const r = await shopifyGraphQL(q);
    res.json(r.data.currentBulkOperation || {status:'NONE'});
  } catch(err){
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   BULK DOWNLOAD (Correct Filtering)
===================================================== */

app.get('/api/bulk/download', async (req,res)=>{
  try{

    const q = `{ currentBulkOperation { status url } }`;
    const r = await shopifyGraphQL(q);
    const op = r.data.currentBulkOperation;

    if (!op || op.status !== 'COMPLETED')
      return res.status(400).json({error:'Bulk not ready'});

    const fetchRes = await fetch(op.url);

    const gunzip = zlib.createGunzip();
    stream.pipeline(fetchRes.body, gunzip, ()=>{});

    const rl = readline.createInterface({ input: gunzip });

    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition','attachment; filename="bulk-orders.csv"');

    const headers = ['order_number','created_at','utm_source','utm_medium','utm_campaign','utm_term','utm_content'];
    res.write(headers.join(',') + '\n');

    for await (const line of rl){

      if(!line.trim()) continue;

      const parsed = JSON.parse(line);

      // Skip nested child objects
      if(parsed.__parentId) continue;

      // Ensure only Order objects
      if(!parsed.id || !parsed.id.includes('gid://shopify/Order/')) continue;

      const row = mapOrder(parsed);

      const csvLine = headers.map(k =>
        `"${(row[k] || '').replace(/"/g,'""')}"`
      ).join(',');

      res.write(csvLine + '\n');
    }

    res.end();

  } catch(err){
    res.status(500).json({error:err.message});
  }
});

/* =====================================================
   START SERVER
===================================================== */

const PORT = process.env.PORT || 3000;
app.listen(PORT, ()=>console.log('Server running on', PORT));
