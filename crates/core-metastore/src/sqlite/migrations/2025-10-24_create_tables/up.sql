CREATE TABLE IF NOT EXISTS volumes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ident TEXT NOT NULL UNIQUE,
    volume_type TEXT NOT NULL CHECK(volume_type IN ('s3', 's3_tables', 'file', 'memory')) NOT NULL,
    volume TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS databases (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ident TEXT NOT NULL UNIQUE,
    properties TEXT,
    volume_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (volume_id) REFERENCES volumes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schemas (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ident TEXT NOT NULL UNIQUE,
    database_id INTEGER NOT NULL,
    properties TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tables (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ident TEXT NOT NULL UNIQUE,
    metadata TEXT NOT NULL,
    metadata_location TEXT NOT NULL,
    properties TEXT NOT NULL,
    volume_ident TEXT,
    volume_location TEXT,
    is_temporary BOOLEAN NOT NULL,
    format TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL    
);

CREATE INDEX IF NOT EXISTS idx_databases ON databases(ident, volume_id, created_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_schemas ON schemas(ident, created_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_tables ON tables(ident, created_at, updated_at);
