# Makefile for Rust Project

# Variables
CARGO ?= cargo

# Default target
all: build

# Build the project in debug mode
build:
	$(CARGO) build

# Build the project in release mode
release:
	$(CARGO) build --release

# Run tests
test:
	$(CARGO) test


integration-test: build
	cd tests && \
	if [ ! -d "venv" ]; then \
		python3 -m venv venv; \
	fi && \
	. venv/bin/activate && \
	pip install --upgrade pip setuptools wheel && \
	pip install -r requirements.txt && \
	pytest

# Install project as binary (if applicable)
install:
	$(CARGO) install --path .

# Uninstall project binary (if applicable)
uninstall:
	$(CARGO) uninstall $(shell $(CARGO) read-manifest | jq -r .name)



# PHONY targets
.PHONY: all build release run run-release test test-verbose clean format format-check lint doc update install uninstall bench examples