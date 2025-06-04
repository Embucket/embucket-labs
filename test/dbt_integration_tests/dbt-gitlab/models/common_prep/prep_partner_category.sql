{{ config(
    tags=["mnpi_exception"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('prep_crm_opportunity') }}
    WHERE is_live

), sfdc_account_source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), partner_category AS (


   SELECT DISTINCT 
     {{ partner_category('source.sales_qualified_source', 'fulfillment_partner.account_name') }} AS partner_category 
   FROM source
   LEFT JOIN sfdc_account_source      AS fulfillment_partner
     ON source.fulfillment_partner = fulfillment_partner.account_id

), unioned AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['partner_category']) }}  AS dim_partner_category_id,
      partner_category                                     AS partner_category_name
    FROM partner_category

    UNION ALL
    
    SELECT
      MD5('-1')                                              AS dim_partner_category_id,
      'Missing order_type_name'                              AS partner_category_name

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-03-10",
    updated_date="2023-03-10"
) }}
