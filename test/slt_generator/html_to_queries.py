import os
import json
from bs4 import BeautifulSoup


def extract_sql_query(div):
    """Extract SQL query from a specific div."""
    sql_div = div
    sql_queries = []

    if not sql_div:
        return None, False

    sql_text = sql_div.get_text(strip=False)
    current_query = []
    in_result = False

    for line in sql_text.splitlines():
        stripped_line = line.strip()

        if "+-" in line or "-+" in line:
            in_result = True
            continue

        if line.startswith("--"):
            continue

        if in_result and stripped_line == "":
            in_result = False
            continue

        if not in_result:
            current_query.append(line)

            if stripped_line.endswith(";"):
                completed_query = "\n".join(current_query).strip()
                sql_queries.append(completed_query)
                current_query = []

    return "\n".join(sql_queries).strip()


def extract_sql_statements(file_path):
    """Extract SQL queries and category from HTML files."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            print(f"Processing File: {file_path}")
            soup = BeautifulSoup(file, "html.parser")
            queries = []

            divs = []
            for section in soup.find_all("section", id=lambda x: x and "example" in x):
                divs.extend(section.find_all("div", class_="highlight-sqlexample notranslate"))

            if not divs:
                return None

            category_elements = soup.select("a.text-link.hover\\:underline")
            category = category_elements[-1].get_text(strip=True) if category_elements else "unknown-category"

            if category == "FINETUNE":
                category = "String & binary"
            if category == "ALTER APPLICATION PACKAGE":
                category = "Native Apps Framework"
            if category in ("CREATE CATALOG INTEGRATION",
                            "CREATE SECURITY INTEGRATION",
                            "ALTER SECURITY INTEGRATION",
                            "Listings"):
                category = "Integrations"
            if category in ("CREATE", "INSERT"):
                category = "General DDL"
            if category in ("CREATE TABLE", "ALTER TABLE"):
                category = "Tables, views, & sequences"
            if category in ("CREATE ICEBERG TABLE", "ALTER ICEBERG TABLE"):
                category = "Tables, views, & sequences"

            for div in divs:
                query_text = extract_sql_query(div)

                if query_text:
                    queries.append(query_text)

            return {
                "page_name": file_path.split("/")[-1].split(".")[0],
                "category": category,
                "queries": queries
            }
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def run():
    directory = "documentation-snapshot/sql-reference-functions"
    specific_page_only = None

    if not specific_page_only:
        specific_page_only = ""
    json_file_path = f"parsed_queries{specific_page_only}.json"


    structured_data = {}

    if os.path.exists(json_file_path):
        with open(json_file_path, "r", encoding="utf-8") as json_file:
            try:
                structured_data = json.load(json_file)
            except json.JSONDecodeError:
                print("Error: Could not decode JSON file. Starting fresh.")
                structured_data = {}

    if os.path.exists(directory):
        root_folder = os.path.basename(directory)
        if root_folder not in structured_data:
            structured_data[root_folder] = {}

        existing_queries = set()
        for category, queries in structured_data[root_folder].items():
            if isinstance(queries, list):
                # Handle the case where queries is a list of dictionaries
                for query in queries:
                    if isinstance(query, dict) and "query_text" in query and "page_name" in query:
                        existing_queries.add((query["query_text"], query["page_name"]))
            elif isinstance(queries, dict):
                # Handle the case where queries itself is a dictionary with nested structure
                for subcategory, subqueries in queries.items():
                    if isinstance(subqueries, list):
                        for query in subqueries:
                            if isinstance(query, dict) and "query_text" in query and "page_name" in query:
                                existing_queries.add((query["query_text"], query["page_name"]))

        visited_pages = set()
        for root, _, files in os.walk(directory):
            html_files = [f for f in files if f.endswith(".html")]
            files_with_mtime = [(os.path.join(root, f), os.path.getmtime(os.path.join(root, f))) for f in html_files]
            files_with_mtime.sort(key=lambda x: x[1])

            for file_path, _ in files_with_mtime:
                if specific_page_only and specific_page_only not in file_path:
                    continue
                file_data = extract_sql_statements(file_path)

                if file_data and file_data["queries"]:
                    category = file_data["category"]
                    page_name = file_data["page_name"]
                    visited_pages.add(page_name)

                    for query_text in file_data["queries"]:
                        if (query_text, page_name) not in existing_queries:
                            entry = {"query_text": query_text, "page_name": page_name}

                            if category not in structured_data[root_folder]:
                                structured_data[root_folder][category] = {}
                            if page_name not in structured_data[root_folder][category]:
                                structured_data[root_folder][category][page_name] = []
                            structured_data[root_folder][category][page_name].append(entry)
                            existing_queries.add((query_text, page_name))

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(structured_data, json_file, indent=4, ensure_ascii=False)
        print(f"Queries saved to {json_file_path}")
    else:
        print(f"Error: Directory '{directory}' not found.")


if __name__ == "__main__":
    run()
