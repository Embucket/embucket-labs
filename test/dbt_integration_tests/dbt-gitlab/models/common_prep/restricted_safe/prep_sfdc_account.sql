{{ config({
        "materialized": "view",
    })
}}

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), sfdc_account_with_ultimate_parent AS (

    SELECT
      sfdc_account.account_id                                                                                        AS dim_crm_account_id,
      sfdc_account.ultimate_parent_account_id                                                                        AS ultimate_parent_account_id,
      sfdc_account.account_sales_segment                                                                             AS ultimate_parent_sales_segment_name,
      sfdc_account.parent_account_industry_hierarchy                                                                 AS ultimate_parent_industry,
      sfdc_account.account_territory                                                                    AS ultimate_parent_territory,
      CASE 
        WHEN ultimate_parent_sales_segment_name IN ('Large', 'PubSec')
          THEN 'Large'
        ELSE ultimate_parent_sales_segment_name
      END                                                                                                            AS ultimate_parent_sales_segment_grouped,
      sfdc_account.billing_country,
      sfdc_account.industry
    FROM sfdc_account

), final AS (

    SELECT
      dim_crm_account_id                                                                                             AS dim_crm_account_id,
      ultimate_parent_territory                                                                                      AS dim_parent_sales_territory_name_source,
      ultimate_parent_account_id                                                                                     AS dim_parent_crm_account_id,
      ultimate_parent_sales_segment_name                                                                             AS dim_parent_sales_segment_name_source,
      ultimate_parent_sales_segment_grouped                                                                          AS dim_parent_sales_segment_grouped_source,
      industry                                                                                                       AS dim_account_industry_name_source,
      ultimate_parent_industry                                                                                       AS dim_parent_industry_name_source,
      billing_country                                                                                                AS dim_account_location_country_name_source
    FROM sfdc_account_with_ultimate_parent

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@lisvinueza",
    created_date="2020-10-30",
    updated_date="2023-05-21"
) }}
