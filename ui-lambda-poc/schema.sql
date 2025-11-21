-- Embucket UI Lambda POC - Database Schema
-- Run this script to set up the required Postgres tables

-- Create database (run this separately if needed)
-- CREATE DATABASE embucket-poc-db;

-- Connect to the database
-- \c embucket-poc-db

-- Table for table metadata
CREATE TABLE IF NOT EXISTS tables_metadata (
  id SERIAL PRIMARY KEY,
  database_name VARCHAR(255),
  schema_name VARCHAR(255),
  table_name VARCHAR(255),
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(database_name, schema_name, table_name)
);

-- Insert sample data
INSERT INTO tables_metadata (database_name, schema_name, table_name)
VALUES 
  ('embucket', 'public', 'sales'),
  ('embucket', 'public', 'users'),
  ('embucket', 'public', 'orders'),
  ('analytics', 'public', 'products'),
  ('analytics', 'public', 'events')
ON CONFLICT (database_name, schema_name, table_name) DO NOTHING;

-- Verify data
SELECT * FROM tables_metadata;
