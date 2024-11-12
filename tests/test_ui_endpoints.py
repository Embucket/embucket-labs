import json
import urllib

import pandas as pd
import pyarrow as pa
import requests


def test_query_endpoint(server, catalog, namespace):
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    table = catalog.load_table((*namespace.name, table_name))

    df = pd.DataFrame(
        {
            "my_ints": [1, 2, 3],
            "my_floats": [1.1, 2.2, 3.3],
            "strings": ["a", "b", "c"],
        }
    )
    data = pa.Table.from_pandas(df)
    table.append(data)

    list_warehouses_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses"
    )
    response = requests.get(list_warehouses_url)
    wh_id = response.json()['warehouses'][0]['id']

    query_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/{table_name}/query"
    )

    response = requests.post(query_table_url, json={
        "query": f"SELECT strings FROM catalog.`{namespace.name[0]}`.`{table_name}` WHERE my_ints = 2;"
    })
    response_data = response.json()
    result = json.loads(response_data['result'])

    assert response.status_code == 200
    assert result == [{"strings": "b"}]
    assert 0 < response_data['durationSeconds'] < 1

    response = requests.post(query_table_url, json={
        "query": f"SELECT SUM(my_ints) FROM catalog.`{namespace.name[0]}`.`{table_name}`;"
    })
    result = json.loads(response.json()['result'])

    assert response.status_code == 200
    assert result == [{f"sum(catalog.{namespace.name[0]}.{table_name}.my_ints)": sum(df["my_ints"])}]

    response = requests.post(query_table_url, json={
        "query": "INCORRECT SQL"
    })
    assert response.status_code == 422
    assert "SQL error: ParserError" in response.text


def test_upload_endpoint(server, catalog, namespace):
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    table = catalog.load_table((*namespace.name, table_name))

    # Table is empty right after creation
    assert table.scan().to_arrow().to_pandas().empty

    list_warehouses_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses"
    )
    response = requests.get(list_warehouses_url)
    wh_id = response.json()['warehouses'][0]['id']

    upload_to_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/{table_name}/upload"
    )

    file = open('test_table_data_wrong_schema.csv', 'rb')
    original_data = pd.read_csv('test_table_data_wrong_schema.csv')
    files = {'upload_file': file}
    response = requests.post(upload_to_table_url, files=files)

    # This error happens when you try to load data that does not fit in the table's schema
    assert response.status_code == 422
    assert "Parquet error: Incorrect number of rows" in response.text  # Error code is not very helpful for now

    file = open('test_table_data.csv', 'rb')
    original_data = pd.read_csv('test_table_data.csv')
    files = {'upload_file': file}
    response = requests.post(upload_to_table_url, files=files)

    # Successfully uploaded data from file
    assert response.status_code == 200

    table = catalog.load_table((*namespace.name, table_name))

    # Data from table equals data from file
    assert table.scan().to_arrow().to_pandas().equals(original_data)

    query_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/{table_name}/query"
    )
    response = requests.post(query_table_url, json={
        "query": f"SELECT SUM(my_ints) FROM catalog.`{namespace.name[0]}`.`{table_name}`;"
    })
    result = json.loads(response.json()['result'])

    # Querying using our API also works
    assert response.status_code == 200
    assert result == [{f"sum(catalog.{namespace.name[0]}.{table_name}.my_ints)": 10}]
