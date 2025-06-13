{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','companies_and_ebrs') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR                               AS _fivetran_id,
    _fivetran_deleted::BOOLEAN                          AS _fivetran_deleted,
    company_pubsec_type::VARCHAR                        AS company_pubsec_type,
    company_csm_manager_name::VARCHAR                   AS company_csm_manager_name,
    cta_company_gsid::VARCHAR                           AS cta_company_gsid,
    company_account_demographics_role_type::VARCHAR     AS company_account_demographics_role_type,
    cta_company_name::VARCHAR                           AS cta_company_name,
    company_arr::FLOAT                                  AS company_arr,
    cta_created_date::TIMESTAMP                         AS cta_created_date,
    company_name::VARCHAR                               AS company_name,
    company_csm_name::VARCHAR                           AS company_csm_name,
    company_gsid::VARCHAR                               AS company_gsid,
    company_account_demographics_geo::VARCHAR           AS company_account_demographics_geo,
    cta_start_date::TIMESTAMP                           AS cta_start_date,
    cta_max_of_closed_date::TIMESTAMP                   AS cta_max_of_closed_date,
    cta_closed_date::TIMESTAMP                          AS cta_closed_date,
    company_original_contract_date::DATE                AS company_original_contract_date,
    company_csm_prioritization::NUMBER                  AS company_csm_prioritization,
    company_sales_segment::VARCHAR                      AS company_sales_segment,
    cta_owner_name::VARCHAR                             AS cta_owner_name,
    cta_type_name::VARCHAR                              AS cta_type_name,
    cta_status_name::VARCHAR                            AS cta_status_name,
    cta_csm_name::VARCHAR                               AS cta_csm_name,
    company_account_demographics_business_unit::VARCHAR AS company_account_demographics_business_unit,
    cta_nr_of_days_past_ebr_closed_success::NUMBER      AS cta_nr_of_days_past_ebr_closed_success,
    cta_name::VARCHAR                                   AS cta_name,
    company_account_demographics_region::VARCHAR        AS company_account_demographics_region,
    company_public_sector_account::BOOLEAN              AS company_public_sector_account,
    cta_gsid::VARCHAR                                   AS cta_gsid,
    _fivetran_synced::TIMESTAMP                         AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
