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


def test_simple_dbt_workload(server, catalog, namespace):
    list_warehouses_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses"
    )
    response = requests.get(list_warehouses_url)
    wh_id = response.json()['warehouses'][0]['id']
    wh_name = response.json()['warehouses'][0]['name']

    customers_raw_table_name = "customers_raw"
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("first_name", pa.string()),
            pa.field("last_name", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, customers_raw_table_name), schema=schema)

    orders_raw_table_name = "orders_raw"
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("user_id", pa.int64()),
            pa.field("order_date", pa.date32()),
            pa.field("status", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, orders_raw_table_name), schema=schema)

    upload_to_customers_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/{customers_raw_table_name}/upload"
    )

    file = open('jaffle_shop_customers.csv', 'rb')
    files = {'upload_file': file}
    response = requests.post(upload_to_customers_table_url, files=files)

    assert response.status_code == 200

    upload_to_orders_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/"
                               f"{orders_raw_table_name}/upload"
    )

    file = open('jaffle_shop_orders.csv', 'rb')
    files = {'upload_file': file}
    response = requests.post(upload_to_orders_table_url, files=files)

    assert response.status_code == 200

    query_table_url = urllib.parse.urljoin(
        server.management_url, f"ui/warehouses/{wh_id}/databases/{namespace.name[0]}/tables/"
                               f"{customers_raw_table_name}/query"
    )

    # Original:
    # create or replace transient table analytics.dbt_AO.stg_customers
    #     as
    # (select
    #  id as customer_id,
    #  first_name,
    #  last_name
    #
    #  from raw.jaffle_shop.customers
    #  );
    customers_stg_table_name = "stg_customers"
    customers_raw_table_ident = f"`{wh_name}`.`{namespace.name[0]}`.`{customers_raw_table_name}`"
    customers_stg_table_ident = f"`{wh_name}`.`{namespace.name[0]}`.`{customers_stg_table_name}`"
    response = requests.post(query_table_url, json={
        "query": f"CREATE OR REPLACE TABLE {customers_stg_table_ident} as (SELECT id as customer_id, first_name, "
                 f"last_name FROM  {customers_raw_table_ident});"
    })
    assert response.status_code == 200

    # Original:
    # create or replace transient table analytics.dbt_AO.stg_orders
    #     as
    # (select
    #  id as order_id,
    #  user_id as customer_id,
    #  order_date,
    #  status
    #
    #  from raw.jaffle_shop.orders
    #  );
    orders_stg_table_name = "stg_orders"
    orders_raw_table_ident = f"`{wh_name}`.`{namespace.name[0]}`.`{orders_raw_table_name}`"
    orders_stg_table_ident = f"`{wh_name}`.`{namespace.name[0]}`.`{orders_stg_table_name}`"
    response = requests.post(query_table_url, json={
        "query": f"CREATE OR REPLACE TABLE {orders_stg_table_ident} as (SELECT id as order_id, user_id as  "
                 f"customer_id, order_date, status FROM  {orders_raw_table_ident});"
    })
    assert response.status_code == 200

    # Original:
    # create or replace transient table analytics.dbt_AO.customers
    #     as
    # (with customers as (
    #
    #  select * from analytics.dbt_AO.stg_customers
    #
    #  ),
    #
    # orders as (
    #
    #         select * from analytics.dbt_AO.stg_orders
    #
    # ),
    #
    # customer_orders as (
    #
    #     select
    #     customer_id,
    #
    #     min(order_date) as first_order_date,
    # max(order_date) as most_recent_order_date,
    # count(order_id) as number_of_orders
    #
    # from orders
    #
    #     group by 1
    #
    # ),
    #
    # final as (
    #
    #     select
    #     customers.customer_id,
    #     customers.first_name,
    #     customers.last_name,
    #     customer_orders.first_order_date,
    #     customer_orders.most_recent_order_date,
    #     coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    #
    # from customers
    #
    #     left join customer_orders using (customer_id)
    #
    # )
    #
    # select * from final
    # );
    customers_final_table_name = "customers"
    customers_final_table_ident = f"`{wh_name}`.`{namespace.name[0]}`.`{customers_final_table_name}`"
    response = requests.post(query_table_url, json={
        "query": f"CREATE OR REPLACE TABLE {customers_final_table_ident} as "
                 f"(with customers as (select * from {customers_stg_table_ident}),"
                 f"orders as (select * from {orders_stg_table_ident}),"
                 f"customer_orders as (select customer_id, min(order_date) as first_order_date, max(order_date) as "
                 f"most_recent_order_date, count(order_id) as number_of_orders from orders group by 1), "
                 f"final as (select customers.customer_id, customers.first_name, customers.last_name, "
                 f"customer_orders.first_order_date, customer_orders.most_recent_order_date, "
                 f"coalesce(customer_orders.number_of_orders, 0) as number_of_orders from customers "
                 f"left join customer_orders using (customer_id)) "
                 f"select * from final)"
    })
    assert response.status_code == 200

    response = requests.post(query_table_url, json={
        "query": f"SELECT * FROM {customers_final_table_ident};"
    })
    assert response.status_code == 200
    result = json.loads(response.json()['result'])

    result_csv = pd.read_csv('jaffle_shop_customers_final.csv')
    result_pd = pd.read_json(json.dumps(result))
    # When I download file from snowflake there are minor differences from what we get using our app
    result_pd.sort_values(by=['customer_id'], inplace=True, ignore_index=True)
    result_pd.columns = map(str.upper, result_pd.columns)

    assert result_pd.equals(result_csv)
