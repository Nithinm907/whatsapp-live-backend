const { Pool } = require('pg');

// Configure connection using environment variables or local connection string
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:your_password@localhost:5432/chatapp',
});

module.exports = {
  query: (text, params) => pool.query(text, params),
};