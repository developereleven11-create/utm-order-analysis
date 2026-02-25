// Shopify UTM Orders Dashboard — FINAL GRAPHQL VERSION (customerJourneySummary)

const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
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
    console.error("GraphQL Error:", JSON.stringify(json.errors, null, 2));
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

function mapOrder(node){

  const utm = node.customerJourneySummary?.firstVisit?.utmParameters || {};

  return {
    order_number: node.name,
    created_at: formatDateToIST(node.createdAt),
    utm_source: utm.source || '',
    utm_medium: utm.medium || '',
    utm_campaign: utm.campaign || '',
    utm_term: utm.term || '',
    utm_content: utm.content || ''
  };
}

/* =====================================================
   PREVIEW (GraphQL Pagination)
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
              customerJourneySummary {
                firstVisit {
                  utmParameters {
                    source
                    medium
                    campaign
                    term
                    content
                  }
                }
              }
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
              customerJourneySummary {
                firstVisit {
                  utmParameters {
                    source
                    medium
                    campaign
                    term
                    content
                  }
                }
              }
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
   BULK DOWNLOAD
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

      // Skip nested objects
      if(parsed.__parentId) continue;

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
