CREATE TABLE IF NOT EXISTS volumes (
    id TEXT NOT NULL PRIMARY KEY,
    ident TEXT NOT NULL UNIQUE,
    volume TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS databases (
    id TEXT NOT NULL PRIMARY KEY,
    ident TEXT NOT NULL UNIQUE,
    properties TEXT,
    volume_ident TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (volume_ident) REFERENCES volumes(ident) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schemas (
    id TEXT NOT NULL PRIMARY KEY,
    ident TEXT NOT NULL UNIQUE,
    properties TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tables (
    id TEXT NOT NULL PRIMARY KEY,
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
