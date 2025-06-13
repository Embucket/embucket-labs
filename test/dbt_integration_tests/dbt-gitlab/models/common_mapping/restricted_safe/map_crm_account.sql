WITH account_prep AS (

    SELECT *
    FROM {{ ref('prep_sfdc_account') }}

), sales_segment AS (

    SELECT *
    FROM {{ ref('prep_sales_segment') }}

), sales_territory AS (

    SELECT *
    FROM {{ ref('prep_sales_territory') }}

), industry AS (

    SELECT *
    FROM {{ ref('prep_industry') }}

), location_country AS (

    SELECT *
    FROM {{ ref('prep_location_country') }}

), final AS (

    SELECT
      {{ get_keyed_nulls ('account_prep.dim_parent_crm_account_id') }}                              AS dim_parent_crm_account_id,
      {{ get_keyed_nulls ('account_prep.dim_crm_account_id') }}                                     AS dim_crm_account_id,
      {{ get_keyed_nulls ('sales_segment_ultimate_parent.dim_sales_segment_id') }}                  AS dim_parent_sales_segment_id,
      {{ get_keyed_nulls ('sales_territory_ultimate_parent.dim_sales_territory_id') }}              AS dim_parent_sales_territory_id,
      {{ get_keyed_nulls ('industry_ultimate_parent.dim_industry_id') }}                            AS dim_parent_industry_id,
      {{ get_keyed_nulls ('sales_segment.dim_sales_segment_id') }}                                  AS dim_account_sales_segment_id,
      {{ get_keyed_nulls ('sales_territory.dim_sales_territory_id') }}                              AS dim_account_sales_territory_id,
      {{ get_keyed_nulls ('industry.dim_industry_id') }}                                            AS dim_account_industry_id,
      {{ get_keyed_nulls ('location_country.dim_location_country_id::varchar') }}                   AS dim_account_location_country_id,
      {{ get_keyed_nulls ('location_country.dim_location_region_id') }}                             AS dim_account_location_region_id
    FROM account_prep
    LEFT JOIN sales_segment AS sales_segment_ultimate_parent
      ON account_prep.dim_parent_sales_segment_name_source = sales_segment_ultimate_parent.sales_segment_name
    LEFT JOIN sales_territory AS sales_territory_ultimate_parent
      ON account_prep.dim_parent_sales_territory_name_source = sales_territory_ultimate_parent.sales_territory_name
    LEFT JOIN industry AS industry_ultimate_parent
      ON account_prep.dim_parent_industry_name_source = industry_ultimate_parent.industry_name
    LEFT JOIN sales_segment
      ON account_prep.dim_parent_sales_segment_name_source = sales_segment.sales_segment_name
    LEFT JOIN sales_territory
      ON account_prep.dim_parent_sales_territory_name_source = sales_territory.sales_territory_name
    LEFT JOIN industry
      ON account_prep.dim_account_industry_name_source = industry.industry_name
    LEFT JOIN location_country
      ON account_prep.dim_account_location_country_name_source = location_country.country_name

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@lisvinueza",
    created_date="2020-11-23",
    updated_date="2023-05-21"
) }}
