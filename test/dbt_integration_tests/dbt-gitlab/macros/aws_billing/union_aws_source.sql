/*
For each source_table in source_tables:
- SELECT certain columns
- Then UNION with the next source table
*/
{%- macro union_aws_source(source_tables) -%}

{% set all_raw_sql %}
    {% for source_table in source_tables %}

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
      FROM {{ ref(source_table) }}

      {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endset %}

{{ return(all_raw_sql) }}
{%- endmacro -%}
