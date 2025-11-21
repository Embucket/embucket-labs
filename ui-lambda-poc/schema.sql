-- Embucket UI Lambda POC - Database Schema
-- Run this script to set up the required Postgres tables

-- Create database (run this separately if needed)
-- CREATE DATABASE embucket-poc-db;

-- Connect to the database
-- \c embucket-poc-db

-- Table for query history
CREATE TABLE IF NOT EXISTS query_history (
  id SERIAL PRIMARY KEY,
  sql TEXT NOT NULL,
  status VARCHAR(50),
  duration_ms INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Create index for faster queries by created_at
CREATE INDEX IF NOT EXISTS idx_query_history_created_at ON query_history(created_at DESC);

-- Insert sample data (optional)
INSERT INTO query_history (sql, status, duration_ms)
VALUES 
  ('SELECT * FROM sales WHERE revenue > 1000', 'completed', 150),
  ('SELECT COUNT(*) FROM users', 'completed', 45),
  ('SELECT * FROM orders ORDER BY created_at DESC LIMIT 10', 'completed', 230)
ON CONFLICT DO NOTHING;

-- Verify data
SELECT * FROM query_history ORDER BY id DESC LIMIT 10;
