CREATE TABLE IF NOT EXISTS volumes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    volume_type TEXT NOT NULL CHECK(volume_type IN ('s3', 's3_tables', 'file', 'memory')) NOT NULL,
    volume TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS databases (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    volume_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    properties TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (name, volume_id)
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schemas (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    database_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    properties TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (name, database_id)
    FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tables (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    schema_id INTEGER NOT NULL,
    database_id INTEGER NOT NULL,
    volume_id INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE,
    metadata TEXT NOT NULL,
    metadata_location TEXT NOT NULL,
    properties TEXT NOT NULL,
    volume_location TEXT,
    is_temporary BOOLEAN NOT NULL,
    format TEXT NOT NULL CHECK(format IN ('parquet', 'iceberg')) NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (name, schema_id)
    FOREIGN KEY (schema_id) REFERENCES schemas(id) ON DELETE CASCADE
    FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_databases ON databases(name, volume_id, created_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_schemas ON schemas(name, database_id, created_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_tables ON tables(name, schema_id, created_at, updated_at);
