# Start from the official Rust image
FROM rust:1.85

# Install essential build tools, clang/llvm, and SQLite dependencies
RUN apt-get update && \
    apt-get install -y \
    clang libclang-dev llvm \
    wget unzip build-essential tcl-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Define SQLite version to install (3.45.3 as an example, which is > 3.44.0)
# You can update these ARGs if a newer SQLite version is needed/preferred
ARG SQLITE_YEAR=2025
ARG SQLITE_FILENAME_VERSION=3490200
ARG SQLITE_TARBALL_FILENAME=sqlite-autoconf-${SQLITE_FILENAME_VERSION}.tar.gz

# Download, compile, and install SQLite from source
RUN cd /tmp && \
    wget "https://www.sqlite.org/${SQLITE_YEAR}/${SQLITE_TARBALL_FILENAME}" && \
    tar xvfz "${SQLITE_TARBALL_FILENAME}" && \
    cd "sqlite-autoconf-${SQLITE_FILENAME_VERSION}" && \
    ./configure --prefix=/usr/local \
                CFLAGS="-DSQLITE_ENABLE_COLUMN_METADATA=1 \
                        -DSQLITE_ENABLE_LOAD_EXTENSION=1 \
                        -DSQLITE_ENABLE_FTS5=1 \
                        -DSQLITE_ENABLE_DBSTAT_VTAB=1 \
                        -DSQLITE_ENABLE_NULL_TRIM=1 \
                        -DSQLITE_ENABLE_RTREE=1" && \
    make -j$(nproc) && \
    make install && \
    # Update the linker cache to recognize the new SQLite library
    ldconfig && \
    rm -rf /tmp/*

# Set the working directory in the container
WORKDIR /code

COPY . .

RUN cargo build --example memvfs --features dynamic

CMD ["/bin/bash", "-c", "cat examples/test_memvfs.sql | sqlite3"]
