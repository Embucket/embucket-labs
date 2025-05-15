import json
import openai
from collections import defaultdict
from dotenv import load_dotenv
import os

load_dotenv()

# OpenAI API setup
client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))


def process_queries_with_llm(queries):
    system_prompt = f"""You are an expert Snowflake SQL query optimizer and error checker. 
Your task is to analyze a sequence of Snowflake SQL queries and ensure they can be executed sequentially without errors.
If you identify potential execution errors, such as missing tables or columns, you must correct the problematic
queries or add necessary CREATE OR REPLACE statements to resolve the issues with missing tables, or add `USE SCHEMA COMPATIBILITY_TESTS.PUBLIC;` 
statements when queries start with the USE SCHEMA statement that changes schema to something else.

**Input:**
You will receive a list of Snowflake SQL queries, presented in their intended order of execution.

**Process:**
1. **Sequential Analysis:** Examine the queries in the given order.
2. **Dependency Check:** Identify dependencies between queries, such as table creation and subsequent usage.
3. **Error Detection:** Detect potential execution errors, including:
   - Missing tables or views.
   - Missing columns.
   - Incorrect data types.
   - Syntax errors.
4. **Error Correction:**
   - If a table or view is missing, add a `CREATE OR REPLACE TABLE` or `CREATE OR REPLACE VIEW` statement before the query that uses it.
   - If a column is missing, add it to the `CREATE OR REPLACE TABLE` statement or modify the query to use an existing column.
   - Correct any syntax errors.
   - If a temp table is missing, add a `CREATE OR REPLACE TEMP TABLE` statement.
5. **Schema cleaning:** 
   - If a query starts with statement `USE SCHEMA ...`, add the statement `USE SCHEMA COMPATIBILITY_TESTS.PUBLIC;`
    right before the next `CREATE OR REPLACE TABLE` statement in the list.
6. **Error Handling:**
   - If you cannot fix a query (like with missing tags, function is not valid or feature which requires Enterprise level Snowflake account etc),
    return the original query unchanged.
   - Do not return error messages in place of queries.
**Output Format:**
- Return the updated queries as a JSON array with each query as a string element.
"""

    user_prompt = f"""Here are the queries:\n{'\n'.join(queries)}\n\nReturn the corrected queries as a JSON array of strings.
                No additional text or comments are required on your part."""
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.2,
        top_p=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    )

    response_content = json.loads(response.choices[0].message.content)

    updated_queries = response_content.get("queries", queries) # Fallback to original queries if no queries are returned

    return updated_queries


def process_queries(json_data):
    """Modify queries using LLM and save updated queries per page."""
    for category, subcategories in json_data.items():
        for subcategory, pages in subcategories.items():
            for page_name, queries in pages.items():
                if not queries:
                    continue

                pages_dict = defaultdict(list)

                # Organize queries by page_name
                for query in queries:
                    pages_dict[query["page_name"].strip()].append(query["query_text"].strip())

                updated_queries = {}

                for page, queries_on_page in pages_dict.items():
                    processed_queries = process_queries_with_llm(queries_on_page)
                    # Save processed queries as a list of dictionaries
                    updated_queries[page] = [
                        {"query_text": q.strip(), "page_name": page} for q in processed_queries
                    ]
                json_data[category][subcategory][page_name] = (
                    updated_queries.get(page_name, [])
                )

    return json_data


def main():
    with open("parsed_queries.json", "r") as f:
        queries_data = json.load(f)

    updated_data = process_queries(queries_data)

    with open("enriched_queries.json", "w") as f:
        json.dump(updated_data, f, indent=4)

    print("Queries have been processed and saved to enriched_queries.json.")

if __name__ == "__main__":
    main()
