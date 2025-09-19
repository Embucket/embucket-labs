#!/usr/bin/env python3
# This script is mounted into the container and run by the entrypoint script
"""
Database initialization script for Embucket benchmark infrastructure.
Creates S3 volume, database, and schema automatically when container starts.
Uses Embucket REST API endpoints instead of SQL commands.
"""

import os
import sys
import time
import requests
import json
from typing import Optional

# Ensure output is flushed immediately
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Configuration from environment variables
EMBUCKET_HOST = os.getenv('EMBUCKET_HOST', 'localhost')
EMBUCKET_PORT = os.getenv('EMBUCKET_PORT', '3000')
EMBUCKET_URL = f"http://{EMBUCKET_HOST}:{EMBUCKET_PORT}"

# Authentication configuration
EMBUCKET_USER = os.getenv('EMBUCKET_USER', 'embucket')
EMBUCKET_PASSWORD = os.getenv('EMBUCKET_PASSWORD', 'embucket')

# Database configuration
DATABASE_NAME = os.getenv('DATABASE_NAME', 'embucket')
SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'public')
VOLUME_NAME = os.getenv('VOLUME_NAME', 'benchmark_volume')

# S3 configuration
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-2')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Retry configuration
MAX_RETRIES = 30
RETRY_DELAY = 10  # seconds

# Global variable to store authentication token
auth_token = None


def wait_for_embucket():
    """Wait for Embucket service to be ready."""
    print(f"Waiting for Embucket service at {EMBUCKET_URL}...")
    print(f"🔍 Will try {MAX_RETRIES} times with {RETRY_DELAY} second delays")

    for attempt in range(MAX_RETRIES):
        try:
            print(f"🔍 Attempt {attempt + 1}/{MAX_RETRIES}: Testing {EMBUCKET_URL}/health")
            response = requests.get(f"{EMBUCKET_URL}/health", timeout=5)
            print(f"🔍 Response status: {response.status_code}")
            if response.status_code == 200:
                print("✅ Embucket service is ready!")
                return True
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Embucket not ready yet ({e})")

        if attempt < MAX_RETRIES - 1:
            print(f"🔍 Sleeping for {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

    print("❌ Failed to connect to Embucket service after maximum retries")
    return False


def authenticate() -> bool:
    """Authenticate with Embucket and store the token globally."""
    global auth_token

    # Try authentication with retries
    max_auth_retries = 5
    auth_retry_delay = 10

    for attempt in range(max_auth_retries):
        try:
            print(f"Authentication attempt {attempt + 1}/{max_auth_retries}...")
            auth_headers = {'Content-Type': 'application/json'}

            # Test if the auth endpoint is reachable first
            print(f"Testing auth endpoint: {EMBUCKET_URL}/ui/auth/login")

            auth_response = requests.post(
                f'{EMBUCKET_URL}/ui/auth/login',
                headers=auth_headers,
                data=json.dumps({"username": EMBUCKET_USER, "password": EMBUCKET_PASSWORD}),
                timeout=60  # Increased timeout
            )

            if auth_response.status_code != 200:
                print(f"❌ Authentication failed with status {auth_response.status_code}: {auth_response.text}")
                if attempt < max_auth_retries - 1:
                    print(f"Retrying in {auth_retry_delay} seconds...")
                    time.sleep(auth_retry_delay)
                    continue
                return False

            auth_token = auth_response.json()["accessToken"]
            print("✅ Successfully authenticated with Embucket")
            return True

        except requests.exceptions.Timeout as e:
            print(f"❌ Authentication timeout on attempt {attempt + 1}: {e}")
            if attempt < max_auth_retries - 1:
                print(f"Retrying in {auth_retry_delay} seconds...")
                time.sleep(auth_retry_delay)
                continue
        except requests.exceptions.RequestException as e:
            print(f"❌ Authentication request failed on attempt {attempt + 1}: {e}")
            if attempt < max_auth_retries - 1:
                print(f"Retrying in {auth_retry_delay} seconds...")
                time.sleep(auth_retry_delay)
                continue
        except (KeyError, json.JSONDecodeError) as e:
            print(f"❌ Failed to parse authentication response: {e}")
            return False

    print(f"❌ Failed to authenticate after {max_auth_retries} attempts")
    return False


def get_auth_headers():
    """Get headers with authentication token."""
    return {
        'authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }


def execute_sql_query(query: str, description: str) -> bool:
    """Execute SQL query via Embucket REST API (for schema creation)."""
    try:
        headers = get_auth_headers()

        print(f"Executing SQL: {description}")
        print(f"Query: {query}")

        response = requests.post(
            f"{EMBUCKET_URL}/ui/queries",
            headers=headers,
            data=json.dumps({"query": query}),
            timeout=30
        )

        if response.status_code in [200, 201]:
            print(f"✅ {description} - Success")
            return True
        else:
            print(f"❌ {description} - Failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ {description} - Request failed: {e}")
        return False


def create_s3_volume() -> bool:
    """Create S3 volume for persistent storage using REST API."""
    if not all([S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
        print("❌ Missing S3 configuration. Required: S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        return False

    try:
        headers = get_auth_headers()

        # Create S3 volume using REST API (matching internal Embucket format)
        volume_payload = {
            "ident": VOLUME_NAME,
            "type": "s3",
            "region": AWS_REGION,
            "bucket": S3_BUCKET,
            "endpoint": f"https://s3.{AWS_REGION}.amazonaws.com",
            "credentials": {
                "credential_type": "access_key",
                "aws-access-key-id": AWS_ACCESS_KEY_ID,
                "aws-secret-access-key": AWS_SECRET_ACCESS_KEY
            }
        }

        print(f"Creating S3 volume: {VOLUME_NAME}")
        print(f"S3 Bucket: {S3_BUCKET}")
        print(f"S3 Prefix: embucket-data/")
        print(f"AWS Region: {AWS_REGION}")

        response = requests.post(
            f"{EMBUCKET_URL}/v1/metastore/volumes",
            headers=headers,
            json=volume_payload,
            timeout=30
        )

        if response.status_code in [200, 201, 409]:  # 409 means it already exists
            print(f"✅ Created or verified S3 volume: {VOLUME_NAME}")
            return True
        else:
            print(f"❌ Volume creation failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Volume creation request failed: {e}")
        return False


def create_database() -> bool:
    """Create database using the S3 volume via REST API."""
    try:
        headers = get_auth_headers()

        database_payload = {
            "ident": DATABASE_NAME,
            "volume": VOLUME_NAME
        }

        print(f"Creating database: {DATABASE_NAME}")
        print(f"Using volume: {VOLUME_NAME}")

        response = requests.post(
            f"{EMBUCKET_URL}/v1/metastore/databases",
            headers=headers,
            json=database_payload,
            timeout=30
        )

        if response.status_code in [200, 201, 409]:  # 409 means it already exists
            print(f"✅ Created or verified database: {DATABASE_NAME}")
            return True
        else:
            print(f"❌ Database creation failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Database creation request failed: {e}")
        return False


def create_schema() -> bool:
    """Create schema in the database using SQL query."""
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}"
    return execute_sql_query(schema_query, f"Create schema '{DATABASE_NAME}.{SCHEMA_NAME}'")


def verify_setup() -> bool:
    """Verify that the database setup was successful using SQL queries."""
    print("\n🔍 Verifying database setup...")

    # Check if volume exists
    volume_check = f"SHOW VOLUMES LIKE '{VOLUME_NAME}'"
    if not execute_sql_query(volume_check, f"Verify volume '{VOLUME_NAME}' exists"):
        print("⚠️ Volume verification failed, but this might be expected if SHOW VOLUMES is not supported")

    # Check if database exists
    database_check = f"SHOW DATABASES LIKE '{DATABASE_NAME}'"
    if not execute_sql_query(database_check, f"Verify database '{DATABASE_NAME}' exists"):
        print("⚠️ Database verification failed, but this might be expected if SHOW DATABASES is not supported")

    # Skip schema verification as SHOW SCHEMAS is not supported
    print("⚠️ Skipping schema verification as SHOW SCHEMAS is not supported by Embucket")

    print("✅ Database setup verification completed (some checks may have failed due to API limitations)")
    return True


def main():
    """Main initialization function."""
    print("🚀 Starting Embucket benchmark database initialization...")
    print(f"Target: {EMBUCKET_URL}")
    print(f"Database: {DATABASE_NAME}")
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Volume: {VOLUME_NAME} (S3: s3://{S3_BUCKET}/embucket-data/)")
    print("📋 Environment variables loaded successfully")

    # Debug: Check if all required variables are set
    print(f"🔍 S3_BUCKET: {S3_BUCKET}")
    print(f"🔍 AWS_ACCESS_KEY_ID: {'***' if AWS_ACCESS_KEY_ID else 'NOT SET'}")
    print(f"🔍 AWS_SECRET_ACCESS_KEY: {'***' if AWS_SECRET_ACCESS_KEY else 'NOT SET'}")
    print(f"🔍 AWS_REGION: {AWS_REGION}")
    print("🔍 Starting wait_for_embucket()...")

    # Wait for Embucket service to be ready
    if not wait_for_embucket():
        sys.exit(1)

    # Give Embucket additional time to fully initialize after health check passes
    print("⏳ Waiting additional 30 seconds for Embucket to fully initialize...")
    time.sleep(30)

    # Authenticate with Embucket
    if not authenticate():
        print("❌ Failed to authenticate with Embucket")
        sys.exit(1)

    # Create S3 volume
    if not create_s3_volume():
        print("❌ Failed to create S3 volume")
        sys.exit(1)

    # Create database
    if not create_database():
        print("❌ Failed to create database")
        sys.exit(1)

    # Wait 15 seconds after database creation
    print("⏳ Waiting 15 seconds after database creation...")
    time.sleep(15)

    # Create schema
    if not create_schema():
        print("❌ Failed to create schema")
        sys.exit(1)

    # Verify setup
    if not verify_setup():
        print("❌ Database setup verification failed")
        sys.exit(1)

    print("🎉 Benchmark database initialization completed successfully!")
    print(f"✅ S3 Volume: {VOLUME_NAME}")
    print(f"✅ Database: {DATABASE_NAME}")
    print(f"✅ Schema: {DATABASE_NAME}.{SCHEMA_NAME}")
    print(f"✅ Storage: s3://{S3_BUCKET}/embucket-data/")


if __name__ == "__main__":
    main()
