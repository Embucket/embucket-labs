WITH crm_account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account') }}

), order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), sales_qualified_source AS (

    SELECT *
    FROM {{ ref('dim_sales_qualified_source') }}

), deal_path AS (

    SELECT *
    FROM {{ ref('dim_deal_path') }}

), sales_segment AS (

    SELECT *
    FROM {{ ref('dim_sales_segment') }}

), opportunity_fields AS(

    SELECT
    dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_user_id,
    deal_path,
    order_type,
    sales_segment,
    sales_qualified_source
  FROM {{ ref('dim_crm_opportunity') }}

), opportunities_with_keys AS (

    SELECT
      opportunity_fields.dim_crm_opportunity_id,
      {{ get_keyed_nulls('opportunity_fields.dim_crm_user_id') }}                                                       AS dim_crm_user_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                             AS dim_order_type_id,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                     AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('deal_path.dim_deal_path_id') }}                                                               AS dim_deal_path_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      crm_account_dimensions.dim_crm_account_id,
      crm_account_dimensions.dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}  AS dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id

    FROM opportunity_fields
    LEFT JOIN crm_account_dimensions
      ON opportunity_fields.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
    LEFT JOIN sales_qualified_source
      ON opportunity_fields.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
    LEFT JOIN order_type
      ON opportunity_fields.order_type = order_type.order_type_name
    LEFT JOIN deal_path
      ON opportunity_fields.deal_path = deal_path.deal_path_name
    LEFT JOIN sales_segment
      ON opportunity_fields.sales_segment = sales_segment.sales_segment_name

)

{{ dbt_audit(
    cte_ref="opportunities_with_keys",
    created_by="@snalamaru",
    updated_by="@lisvinueza",
    created_date="2020-12-17",
    updated_date="2023-05-21"
) }}
