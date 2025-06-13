{% docs aws_source_incremental %}

## Run AWS source incrementally

This macro is designed to run an incremental delete+strategy on each passed in table, and then parse the JSON to select the desired fields.

## Fields:

* `source_table`: (Required) The name of the source table in your AWS billing schema.
* `source_schema`: (Optional, default: 'aws_billing') The schema containing the source table.
* `delete_insert_key`: (Optional, default: `year_mo_partition`) Column to use for dbt incremental delete+insert. If using 'year_mo_partition', this means that any existing year_mo_partitions that are part of the incremental batch are first deleted before being re-inserted incrementally.

**Example Usage:**

{% raw %}
```jinja2
{% set my_aws_incremental_table = aws_source_incremental('dedicated_dev_3675') %}
select * from {{ my_aws_incremental_table }}
```
{% endraw %}
## Generated SQL:

{% raw %}
```sql

WITH source AS (
  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM "RAW".aws_billing.dedicated_dev_3675
),

parsed AS (
  SELECT
    value['bill_bill_type']::VARCHAR                                                   AS bill_bill_type,
    ...
```
{% endraw %}
{% enddocs %}

{% docs union_aws_source %}

## Unioning Multiple AWS Sources

This macro unions data from multiple AWS billing source tables

## Fields:

* `source_tables` (**required**): A list of the names of your AWS source tables (e.g., `['table1', 'table2', 'table3']`).

**Example Usage:**
{% raw %}
```jinja2
{% set source_tables = ['dedicated_legacy_0475',
    'dedicated_dev_3675',
    'gitlab_marketplace_5127',
    'itorg_3027',
    'legacy_gitlab_0347',
    'services_org_6953'] %}

{{ union_aws_source(source_tables)}}
```
{% endraw %}
## Generated SQL:
{% raw %}
```sql
SELECT
  DATE(line_item_usage_start_date) AS date_day, --date
  bill_payer_account_id AS billing_account_id, -- acount id
  bill_billing_period_end_date AS billing_period_end, --invoice month
  line_item_usage_account_id AS sub_account_id, -- project.id eq
  line_item_product_code AS service_name,
  line_item_line_item_description AS charge_description, --sku desc
  line_item_usage_amount AS pricing_quantity, --usage amount in proicing unit
  pricing_unit AS pricing_unit,
  line_item_net_unblended_cost AS billed_cost,
  pricing_public_on_demand_cost AS list_cost
FROM "RAW".aws_billing.dedicated_legacy_0475

UNION ALL

SELECT
  DATE(line_item_usage_start_date) AS date_day, --date
  bill_payer_account_id AS billing_account_id, -- acount id
  bill_billing_period_end_date AS billing_period_end, --invoice month
  line_item_usage_account_id AS sub_account_id, -- project.id eq
  line_item_product_code AS service_name,
  line_item_line_item_description AS charge_description, --sku desc
  line_item_usage_amount AS pricing_quantity, --usage amount in proicing unit
  pricing_unit AS pricing_unit,
  line_item_net_unblended_cost AS billed_cost,
  pricing_public_on_demand_cost AS list_cost
FROM "RAW".aws_billing.dedicated_dev_3675

UNION ALL

...

```
{% endraw %}
{% enddocs %}
