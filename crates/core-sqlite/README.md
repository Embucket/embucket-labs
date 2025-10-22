# SQLITE

This crate derived from https://github.com/maxmcd/s3qlite

### Prerequisites
Linux:
``` sh
sudo apt install llvm-18 libclang-18-dev
```

### Sqlite compile time flags
Provide all the flags in .cargo/config.toml:
```toml
[env]
LIBSQLITE3_FLAGS = "-DSQLITE_THREADSAFE=1"
```