const express = require('express');
const serverless = require('serverless-http');
const { readFileSync, existsSync } = require('fs');
const { join } = require('path');
const pg = require('pg');
const { Pool } = pg;

// --- CONFIGURATION ---
// We strictly require DB credentials now.
let pool = null;
function getPool() {
  if (!pool) {
    if (!process.env.DB_HOST) {
      throw new Error('CRITICAL: DB_HOST environment variable is missing.');
    }
    pool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '5432', 10),
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
      connectionTimeoutMillis: 5000,
    });
  }
  return pool;
}

const app = express();

app.use(express.json());

// CORS Middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// --- STATIC FILES ---
const staticPath = process.env.LAMBDA_TASK_ROOT 
  ? join(process.env.LAMBDA_TASK_ROOT, 'ui', 'dist') 
  : join(__dirname, 'ui', 'dist');

if (existsSync(staticPath)) app.use(express.static(staticPath));

// --- API ROUTES ---

// GET: Fetch real history from DB
app.get('/api/data', async (req, res) => {
  try {
    const client = getPool();
    const result = await client.query(
      'SELECT * FROM query_history ORDER BY id DESC LIMIT 50'
    );
    
    res.json({
      success: true,
      data: { rows: result.rows }
    });
  } catch (error) {
    console.error('Database Error:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message,
      details: 'Check CloudWatch logs for connection details'
    });
  }
});

// POST: Insert real row into DB
app.post('/api/submit', async (req, res) => {
  try {
    const { sql, status, duration_ms } = req.body;
    
    if (!sql) return res.status(400).json({ error: 'SQL is required' });

    const client = getPool();
    const result = await client.query(
      'INSERT INTO query_history (sql, status, duration_ms) VALUES ($1, $2, $3) RETURNING *',
      [sql, status || 'pending', duration_ms || 0]
    );

    res.json({
      success: true,
      message: 'Inserted successfully',
      row: result.rows[0]
    });
  } catch (error) {
    console.error('Insert Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// --- UI FALLBACK ---
app.get(/(.*)/, (req, res) => {
  const indexPath = join(staticPath, 'index.html');
  if (existsSync(indexPath)) {
    res.send(readFileSync(indexPath, 'utf8'));
  } else {
    res.status(404).send('UI not found. Did you run the build script?');
  }
});

// --- HANDLER ---
const handler = serverless(app);
exports.handler = async (event, context) => {
  return await handler(event, context);
};