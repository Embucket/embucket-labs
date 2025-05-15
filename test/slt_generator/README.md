# SQL Logic Tests generation
This is a step-by-step guide to generate SQL Logic Tests (SLT) for Snowflake.
The project contains Python scripts that download HTML pages containing SQL queries from the
Snowflake documentation, extract these SQL queries, fix missing table creation statements
and typos in the documentation using OpenAI LLM integration, and generate `.slt` files.

# SLT Generation Steps
1. Copy `.env_example` inside `slt_generator` folder file and rename it to `.env` file.


2.  Fill the `.env` file:


   -  Snowflake - replace Snowflake credentials in .env file from test to your credentials.
   - `OPENAI_API_KEY`: Your OpenAI API key starting with "sk-"


3. Download HTML pages with SQL example queries from Snowflake documentation
```
python3 reference_to_html.py
```
4. Extract SQL queries from the HTML pages
```
python3 html_to_queries.py
```
5. Update extracted SQL queries using LLM to fix errors/typos in documentation examples
```
python3 enrich_queries_with_llm.py
```
6. Generate `.slt` files
```
python3 queries_to_slt.py
```
