const pg = require('pg');
const { Pool } = pg;

let pool = null;

function getPool() {
  if (!process.env.DB_HOST) return null;
  
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '5432', 10),
      database: process.env.DB_NAME || 'embucket-poc-db',
      user: process.env.DB_USER || 'embucket',
      password: process.env.DB_PASSWORD,
      ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
      max: 1,
    });
  }
  return pool;
}

module.exports = { getPool };

