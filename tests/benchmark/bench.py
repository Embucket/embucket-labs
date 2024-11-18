import requests

# Local
CATALOG = "http://0.0.0.0:3000"
WAREHOUSE_ID = "a36772b4-7e70-437b-a7cf-5ba9eb565d49"
WAREHOUSE_NAME = "embucket"
TABLE = "hits"
DATABASE = "datasets"

# Prod
# CATALOG = "https://api.embucket.com"
# WAREHOUSE_ID = "9f81080e-966b-41d7-b058-f04d70bf22f4"
# WAREHOUSE_NAME = "9f81080e-966b-41d7-b058-f04d70bf22f4"
# TABLE = "hits_partitioned"
# DATABASE = "datasets"

TABLE_PATH = f"`{WAREHOUSE_NAME}`.{DATABASE}.{TABLE}"

# Clear the contents of result.json
open('result.txt', 'w').close()

with open('queries.sql', 'r') as file:
    for row in file.readlines():
        query = row.replace("warehouse_database_table", TABLE_PATH)
        print(f"Running query: {query}")
        response = requests.post(
            f"{CATALOG}/ui/warehouses/{WAREHOUSE_ID}/databases/{DATABASE}/tables/{TABLE}/query",
            json={"query": query.format(TABLE_PATH)},
        )
        with open('result.txt', 'a') as result_file:
            if response.status_code == 200:
                duration = response.json().get('durationSeconds', 0)
            else:
                print(response.content, response.status_code)
                duration = 0
            result_file.write(f"{response.status_code}|{query.strip()}|{duration}\n")
