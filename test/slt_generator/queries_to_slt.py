import os
import json
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import ProgrammingError
import decimal
from datetime import date, time
import re

# Load environment variables
load_dotenv()


def create_directory_structure(root_folder, main_category, category):
    """Create a directory based on the category name."""
    category_folder = os.path.join(root_folder, main_category.replace(" ", "_"), category.replace(" ", "_"))
    os.makedirs(category_folder, exist_ok=True)
    return category_folder


def should_add_exclude_decorator(query, filename=None):
    """Check if the query needs an exclude-from-coverage decorator."""
    skip_list = ['drop.slt', 'create-table.slt', 'drop-table.slt', 'insert.slt', ]

    # Skip the decorator if the file is in the skip list
    if filename and filename in skip_list:
        return False

    # Commands to check after 'statement ok'
    sql_command_pattern = re.compile(
        r'^\s*(CREATE OR REPLACE|DROP|INSERT|USE SCHEMA)\b',
        re.IGNORECASE)

    return sql_command_pattern.match(query.strip()) is not None


def convert_to_sql_value(value):
    """
    Converts Python values to SQL-compatible representations.
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):  # MUST be above int because in Python bool value is also an instance of int type
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, list) or isinstance(value, dict):
        return f"'{json.dumps(value, separators=(',', ':'))}'"
    elif isinstance(value, decimal.Decimal):
        return str(value)  # Convert Decimal to string to retain precision
    elif isinstance(value, str):  # Strings
        if value in {"true", "false", "null"}:
            # Return them as plain strings without parsing
            return value
        try:
            value = value.replace("undefined", "null")
            parsed_json = json.loads(value)  # Try parsing the string as JSON
            if isinstance(parsed_json, (dict, list)):
                return f"'{json.dumps(parsed_json, separators=(',', ':'))}'"
            else:
                return value.replace('\n', '\\n')
        except (ValueError, TypeError):
            # If it's not JSON, return the string as it is
            if value == "":
                return "''"
            return value.replace('\n', '\\n')
    elif isinstance(value, bytearray):  # Binary data
        return f"x'{value.hex()}'"  # Convert to SQL hex literal (e.g., x'1010')
    elif isinstance(value, bytes):  # Regular bytes
        return f"x'{value.hex()}'"
    elif isinstance(value, date) or isinstance(value, time):  # Date and Time
        return f"'{value.isoformat()}'"  # Format as 'YYYY-MM-DD'
    else:
        # Raise an error for unsupported types
        raise TypeError(f"Unsupported data type: {type(value)} for value: {value}")


def execute_query_and_fetch_results(cur, query):
    """
    Execute a query in Snowflake and fetch results.
    """
    try:
        cur.execute(query)
        results = cur.fetchall()
        column_count = len(cur.description)
        output = ["\t".join(convert_to_sql_value(col) for col in row) for row in results]
        return {"type": "query", "expected_result": "\n".join(output), "column_count": column_count}
    except ProgrammingError as e:
        return {"type": "statement error", "error": "\n".join([str(e)])}


def remove_empty_lines(input_string):
    return '\n'.join(line for line in input_string.splitlines() if line.strip())


def generate_slt_file(category, queries, folder_path, sf_cursor):
    """
    Generate an .slt file for a category.
    """
    slt_filename = os.path.join(folder_path, f"{category.replace(' ', '_')}.slt")
    with open(slt_filename, "w") as slt_file:
        for query_obj in queries:
            query = query_obj["query_text"].strip()

            if query.strip().upper().startswith("CREATE") and "OR REPLACE" not in query.upper():
                if query.strip().upper().startswith("CREATE TABLE IF NOT EXISTS"):
                    query = query.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE", -1)
                else:
                    query = query.replace("CREATE TABLE", "CREATE OR REPLACE TABLE", -1)
                    query = query.replace("create table", "CREATE OR REPLACE TABLE", -1)
                    query = query.replace("Create table", "CREATE OR REPLACE TABLE", -1)
                    query = query.replace("CREATE TRANSIENT TABLE", "CREATE OR REPLACE TRANSIENT TABLE", -1)
                    query = query.replace("create transient table", "CREATE OR REPLACE TRANSIENT TABLE", -1)
                    query = query.replace("CREATE FILE", "CREATE OR REPLACE FILE", -1)
                    query = query.replace("create file", "CREATE OR REPLACE FILE", -1)

            # **Handle procedural and EXECUTE IMMEDIATE blocks correctly**
            if re.match(r"(CREATE\s+OR\s+REPLACE\s+PROCEDURE|EXECUTE\s+IMMEDIATE)", query, re.IGNORECASE):
                action = "procedure creation" if "PROCEDURE" in query.upper() else "immediate statement"
                print(f"Executing {action}: {query[:50]}...")

                try:
                    sf_cursor.execute(query)
                    if should_add_exclude_decorator(query):
                        slt_file.write(f"exclude-from-coverage\nstatement ok\n{query}\n\n")
                    else:
                        slt_file.write(f"statement ok\n{query}\n\n")
                except ProgrammingError as e:
                    error_message = "\n".join([str(e)])
                    slt_file.write(f"statement error\n{query}\n----\n{error_message}\n\n")
                    print(f"{action.capitalize()} failed with error: {e}")

                continue

            # **Process standard queries (excluding stored procedures)**
            if query.strip().upper().startswith(("CREATE", "INSERT", "ALTER", "USE", "SET", "BEGIN WORK", "COMMIT WORK")):
                # **Ensure queries are split properly, but NOT inside procedures**
                queries_to_execute = []
                current_query = []
                inside_block = False  # Track procedural blocks

                for line in query.split("\n"):
                    if re.match(r"^\s*BEGIN\s*$", line, re.IGNORECASE):
                        inside_block = True
                    elif re.match(r"^\s*END\s*;\s*$", line, re.IGNORECASE):
                        inside_block = False

                    current_query.append(line)

                    if not inside_block and ";" in line:
                        queries_to_execute.append("\n".join(current_query).strip())
                        current_query = []

                if current_query:
                    queries_to_execute.append("\n".join(current_query).strip())

                for setup_query in queries_to_execute:
                    setup_query_stripped = setup_query.strip()
                    if setup_query_stripped:
                        print(f"Executing setup query: {setup_query_stripped[:50]}...")
                        try:
                            sf_cursor.execute(setup_query_stripped)
                            if should_add_exclude_decorator(setup_query_stripped):
                                slt_file.write(f"exclude-from-coverage\nstatement ok\n{setup_query_stripped}\n\n")
                            else:
                                slt_file.write(f"statement ok\n{setup_query_stripped}\n\n")
                        except ProgrammingError as e:
                            error_message = "\n".join([str(e)])
                            slt_file.write(f"statement error\n{setup_query}\n----\n{error_message}\n\n")
                            print(f"Setup query failed with error: {error_message}")
            else:
                # **Handle queries from "Geospatial" category separately**
                if category == "Geospatial":
                    # Check if the query contains "GEO" and handle semicolons within GeoJSON strings
                    if "GEO" in query.upper():
                        # Split by semicolons that are at the end of the string or followed by a newline
                        geo_queries_split = [q for q in re.split(r';(?=\s*$|\n)', query) if q.strip()]
                        for test_query in geo_queries_split:
                            result = execute_query_and_fetch_results(sf_cursor, test_query)
                            if result["type"] == "statement error":
                                slt_file.write(f"statement error\n{test_query.strip()}\n----\n{result['error']}\n\n")
                            elif result["type"] == "query":
                                expected_results = result["expected_result"]
                                column_count = result["column_count"]
                                descriptor = "T" * column_count
                                slt_file.write(
                                    f"query {descriptor}\n{test_query.strip()}\n----\n{expected_results}\n\n")
                            else:
                                raise ValueError(f"Unexpected result type: {result['type']}")

                        continue

                # **Handle test queries normally**
                print(f"Executing test query: {query[:50]}...")
                for test_query in query.split(';')[:-1]:  # Avoid empty last query
                    result = execute_query_and_fetch_results(sf_cursor, test_query)
                    if result["type"] == "statement error":
                        slt_file.write(f"statement error\n{test_query.strip()}\n----\n{result['error']}\n\n")
                    elif result["type"] == "query":
                        expected_results = result["expected_result"]
                        column_count = result["column_count"]
                        descriptor = "T" * column_count
                        slt_file.write(f"query {descriptor}\n{test_query.strip()}\n----\n{expected_results}\n\n")
                    else:
                        raise ValueError(f"Unexpected result type: {result['type']}")

    print(f"Generated SLT file: {slt_filename}")


def process_json_file(json_filepath, output_root, sf_connection):
    """
    Process the nested JSON structure.
    """
    with open(json_filepath, "r") as json_file:
        data = json.load(json_file)

    sf_cursor = sf_connection.cursor()

    for main_category, subcategories in data.items():
        for category, pages in subcategories.items():
            folder_path = create_directory_structure(output_root, main_category, category)
            for page, queries in pages.items():
                generate_slt_file(page, queries, folder_path, sf_cursor)

    sf_cursor.close()


def run():
    json_file_path = "enriched_queries.json"
    output_folder = "output_slt_files"

    sf_config = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    }

    try:
        print("Connecting to Snowflake...")
        sf_connection = snowflake.connector.connect(**sf_config)

        schema = sf_config.get("schema")
        if schema:
            with sf_connection.cursor() as cursor:
                print(f"Setting schema to '{schema}'...")
                cursor.execute(f"USE SCHEMA {schema};")

        process_json_file(json_file_path, output_folder, sf_connection)
        print(f"All SLT files generated under folder: {output_folder}")

    finally:
        if 'sf_connection' in locals():
            sf_connection.close()


if __name__ == "__main__":
    run()
