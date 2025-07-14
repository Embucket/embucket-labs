import json
import requests
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
REPO_OWNER = "Embucket"
REPO_NAME = "embucket"
BRANCH_NAME = "assets"
FILE_PATH = "assets/test_statistics.csv"
OUTPUT_FILENAME = "combined_csv_file.csv"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

# --- Advanced Configuration ---
API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"


def get_commit_history():
    """
    Fetches the complete commit history for the specified branch,
    handling GitHub API pagination.
    """
    print(f"Fetching commit history for branch: {BRANCH_NAME}...")
    commits_url = f"{API_URL}/commits"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    all_commits = []

    # Initial pagination parameters
    page = 1
    per_page = 100

    while True:
        params = {
            "sha": BRANCH_NAME,
            "path": FILE_PATH,
            "per_page": per_page,
            "page": page
        }

        print(f"Requesting page {page}...")
        response = requests.get(commits_url, headers=headers, params=params)

        if response.status_code == 200:
            page_commits = response.json()

            if not page_commits:
                break

            all_commits.extend(page_commits)
            print(f"Retrieved {len(page_commits)} commits from page {page}")

            if len(page_commits) < per_page:
                break

            page += 1
        else:
            print(f"Error fetching commit history: {response.status_code}")
            print(response.json())
            break

    print(f"Successfully fetched {len(all_commits)} commits total.")
    return all_commits


def get_file_content_at_commit(commit_sha):
    """
    Retrieves the content of the specified file at a given commit.
    """
    contents_url = f"{API_URL}/contents/{FILE_PATH}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    params = {"ref": commit_sha}
    response = requests.get(contents_url, headers=headers, params=params)

    if response.status_code == 200:
        content = requests.get(response.json()["download_url"]).text
        return content
    else:
        return None


def combine_csv_versions(commit_history):
    """
    Downloads all versions of the CSV, combines them, and standardizes columns structure.
    """
    if not commit_history:
        return None

    # Define the expected columns for the final DataFrame
    expected_columns = [
        'page_name', 'category', 'total_tests', 'successful_tests',
        'failed_tests', 'not_implemented_tests', 'coverage_percentage',
        'success_rate_percentage', 'commit_timestamp'
    ]

    all_dfs = []
    print("\nDownloading and processing file versions:")
    for i, commit in enumerate(commit_history):
        commit_sha = commit["sha"]
        commit_date = commit["commit"]["author"]["date"]

        print(f"  ({i + 1}/{len(commit_history)}) Processing commit: {commit_sha[:7]} from {commit_date}...")
        csv_content = get_file_content_at_commit(commit_sha)

        if csv_content:
            try:
                # Parse the CSV content
                df = pd.read_csv(io.StringIO(csv_content))

                # Handle possible column renames/mappings
                column_mapping = {
                    'success_percentage': 'success_rate_percentage'
                }

                # Apply column mapping
                df.rename(columns=column_mapping, inplace=True)

                # Add commit timestamp
                df["commit_timestamp"] = commit_date

                # Fill missing columns with appropriate defaults
                for col in expected_columns:
                    if col not in df.columns:
                        if col in ['not_implemented_tests', 'coverage_percentage']:
                            df[col] = 0  # Default numeric value
                        else:
                            df[col] = None

                # Select only the columns we need
                df = df[expected_columns]

                all_dfs.append(df)
                print(f"    Processed CSV with {len(df)} rows")

            except Exception as e:
                print(f"    Could not process CSV from commit {commit_sha[:7]}: {e}")

    if not all_dfs:
        print("\nNo valid CSV versions found to combine.")
        return None

    print("\nCombining all CSV versions...")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.drop_duplicates(inplace=True)

    print(f"Saving combined data to '{OUTPUT_FILENAME}'...")
    combined_df.to_csv(OUTPUT_FILENAME, index=False)
    print("Combined CSV file created!")
    return combined_df


def upload_csv_to_embucket(csv_data=None, csv_file_path=None):
    """
    Uploads a CSV file or DataFrame to a table in Embucket with page_name_timestamp as primary key.
    """
    protocol = os.getenv('EMBUCKET_PROTOCOL', 'http')
    host = os.getenv('EMBUCKET_HOST', 'localhost')
    port = int(os.getenv('EMBUCKET_PORT', 3000))
    username = os.getenv('EMBUCKET_USER', 'embucket')
    password = os.getenv('EMBUCKET_PASSWORD', 'embucket')
    database = os.getenv('DATABASE', 'embucket')
    schema = os.getenv('SCHEMA', 'public')
    table_name = "test_statistics"

    # Authenticate with Embucket
    auth_headers = {'Content-Type': 'application/json'}
    res = requests.request(
        "POST", f'{protocol}://{host}:{port}/ui/auth/login',
        headers=auth_headers,
        data=json.dumps({"username": username, "password": password})
    ).json()

    headers = {'authorization': f'Bearer {res["accessToken"]}'}

    try:
        # Either use provided DataFrame or read from file
        if csv_data is not None:
            df = csv_data.copy()
        else:
            df = pd.read_csv(csv_file_path)

        # Create the table if it doesn't exist
        table_query = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                    page_name VARCHAR,
                    category VARCHAR,
                    total_tests INTEGER,
                    successful_tests INTEGER,
                    failed_tests INTEGER,
                    not_implemented_tests INTEGER,
                    coverage_percentage FLOAT,
                    success_rate_percentage FLOAT,
                    commit_timestamp VARCHAR,
                    PRIMARY KEY (page_name, commit_timestamp)
                )
                """

        query_headers = headers.copy()
        query_headers['Content-Type'] = 'application/json'

        response = requests.post(
            f"{protocol}://{host}:{port}/ui/queries",
            headers=query_headers,
            data=json.dumps({"query": table_query})
        )

        if response.status_code not in [200, 201]:
            print(f"Failed to create table: {response.text}")
            return False

        print(f"Table {database}.{schema}.{table_name} created or already exists.")

        # Convert DataFrame to CSV bytes
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')

        # Set up upload request
        base_url = f"{protocol}://{host}:{port}/ui/databases/{database}/schemas/{schema}/tables"
        upload_url = f"{base_url}/{table_name}/rows"

        params = {
            "header": "true",
            "delimiter": "44",  # comma delimiter as string
        }

        files = {"file": ("combined_data.csv", csv_bytes, "text/csv")}

        upload_response = requests.post(upload_url, headers=headers, params=params, files=files)

        print(f"Status Code: {upload_response.status_code}")
        print(f"Table Name: {table_name}")

        if upload_response.status_code == 200:
            try:
                json_response = upload_response.json()
                print(f"Successfully uploaded {json_response.get('count', 0)} rows")
                print(f"Upload took {json_response.get('duration_ms', 0)}ms")

            except requests.exceptions.JSONDecodeError:
                print("Response is not valid JSON")
                return False
        else:
            print(f"Upload failed: {upload_response.text}")
            return False
        return True

    except Exception as e:
        print(f"Error during upload process: {str(e)}")
        return False


if __name__ == "__main__":
    # 1. Get commit history
    commits = get_commit_history()

    # 2. Combine CSV versions with timestamps
    combined_df = combine_csv_versions(commits)

    # 3. Upload the combined CSV to Embucket
    if combined_df is not None:
        print("\nUploading combined CSV to Embucket...")
        upload_success = upload_csv_to_embucket(csv_data=combined_df)

        if upload_success:
            print("\nSuccess! All operations completed.")
        else:
            print("\nCombined CSV created but upload to Embucket failed.")
    else:
        print("\nFailed to create combined CSV. Upload skipped.")