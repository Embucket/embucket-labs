query_templates = {
    "default": "SELECT {function}(value) FROM test_table;",
    "dual_arg": "SELECT {function}(arg1, arg2) FROM test_table;",
    "complex": "SELECT {function}(value1, value2, value3) FROM test_table;",
}

custom_queries = {
    "LIKE ANY": "SELECT * FROM like_all_example WHERE name LIKE ANY ('%Jo%oe%','J%n') ORDER BY name;",
    "LIKE ALL": "SELECT * FROM like_all_example WHERE name LIKE ALL ('%Jo%oe%','J%n') ORDER BY name;",
    "LIKE": "SELECT * FROM like_all_example WHERE name LIKE '%Jo%oe%' ORDER BY name;"
}

# Function to generate SQL queries
def generate_queries(function_list):
    queries = []
    for function in function_list:
        if function in custom_queries:
            queries.append(custom_queries[function])
        else:
            queries.append(query_templates["default"].format(function=function))
    return queries

with open('functions_list.txt') as f: functions = f.read()



queries = generate_queries(functions.splitlines())
output_file = "test_queries.sql"
with open(output_file, "w") as f:
    f.write("\n".join(queries))
