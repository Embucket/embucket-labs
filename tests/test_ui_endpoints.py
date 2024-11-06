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

    # TODO create table using API when we support writing functionality
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
        "query": "SELECT strings FROM catalog.`test-namespace`.`my_table` WHERE my_ints = 2;"
    })
    response_data = response.json()
    result = json.loads(response_data['result'])
    assert result == [{"strings": "b"}]
    assert 0 < response_data['durationSeconds'] < 1

    response = requests.post(query_table_url, json={
        "query": "SELECT SUM(my_ints) FROM catalog.`test-namespace`.`my_table`;"
    })
    result = json.loads(response.json()['result'])
    assert result == [{f"sum(catalog.{namespace.name[0]}.{table_name}.my_ints)": sum(df["my_ints"])}]

    response = requests.post(query_table_url, json={
        "query": "INCORRECT SQL"
    })
    assert response.status_code == 422
    assert "SQL error: ParserError" in response.text
