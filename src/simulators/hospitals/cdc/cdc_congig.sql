-- Required: gives Debezium full before+after images on UPDATE
ALTER TABLE hospitals REPLICA IDENTITY FULL;

-- Verify the setting was applied
SELECT relname, relreplident
FROM pg_class
WHERE relname = 'hospitals';
-- relreplident should be 'f' (FULL), not 'd' (DEFAULT)

-- Create a dedicated user for Debezium
CREATE USER debezium_user WITH
  REPLICATION          -- allows reading the WAL
  LOGIN
  PASSWORD 'debezium_pass';

-- Grant the necessary privileges to the Debezium user
GRANT CREATE ON DATABASE medroute_db TO debezium_user;

-- Grant SELECT on the tables to monitor
GRANT SELECT ON hospitals TO debezium_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO debezium_user;