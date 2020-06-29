CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE IF NOT EXISTS managers (
    manager_id SERIAL PRIMARY KEY,
    manager_email VARCHAR(64) UNIQUE NOT NULL,
    manager_hash BYTEA NOT NULL,
    manager_salt VARCHAR(32) NOT NULL,
    api_key UUID UNIQUE NOT NULL,
    is_admin BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TYPE classification_t AS ENUM (
    'confidential',
    'sensitive',
    'private',
    'public'
);

CREATE TYPE format_t AS ENUM (
    'plaintext',
    'json',
    'ndjson',
    'csv',
    'tsv',
    'protobuf'
);

CREATE TYPE compression_t AS ENUM (
    'uncompressed',
    'zip',
    'tar'
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) UNIQUE NOT NULL, 
    dataset_desc TEXT NOT NULL,
    dataset_format format_t NOT NULL, 
    dataset_compression compression_t NOT NULL,
    dataset_classification classification_t NOT NULL,
    dataset_schema hstore NOT NULL,
    manager_id INTEGER NOT NULL REFERENCES managers(manager_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS partitions (
    partition_id SERIAL PRIMARY KEY,
    partition_name VARCHAR(255) NOT NULL,
    partition_url VARCHAR(255) NOT NULL,
    dataset_id INTEGER NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (partition_name, dataset_id)
);  

-- function to automate setting updated_at column on a table
CREATE OR REPLACE FUNCTION on_update_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- apply on_update_set_timestamp function as trigger to initial tables
CREATE TRIGGER auto_update_timestamp
BEFORE UPDATE ON managers
FOR EACH ROW
EXECUTE PROCEDURE on_update_set_timestamp();

CREATE TRIGGER auto_update_timestamp
BEFORE UPDATE ON partitions
FOR EACH ROW
EXECUTE PROCEDURE on_update_set_timestamp();

CREATE TRIGGER auto_update_timestamp
BEFORE UPDATE ON datasets
FOR EACH ROW
EXECUTE PROCEDURE on_update_set_timestamp();