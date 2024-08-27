-- Create table
CREATE TABLE IF NOT EXISTS esus_table (
  id INTEGER PRIMARY KEY, 
  externallogid INTEGER NOT NULL, 
  source TEXT NOT NULL, 
  timestamp TEXT,
  timestamp_tzinfo TEXT, 
  value REAL
);

-- Import data from CSV
.mode csv
.import test-large_20240115_080000_20240307_100157-no-head.csv esus_table
