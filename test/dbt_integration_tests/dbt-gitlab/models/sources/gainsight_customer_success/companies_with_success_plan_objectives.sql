{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','companies_with_success_plan_objectives') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR            AS _fivetran_id,
    _fivetran_deleted::BOOLEAN       AS _fivetran_deleted,
    stage_name::VARCHAR              AS stage_name,
    objective_category_name::VARCHAR AS objective_category_name,
    sales_segment::VARCHAR           AS sales_segment,
    company_status::VARCHAR          AS company_status,
    open_objective_count::VARCHAR    AS open_objective_count,
    name::VARCHAR                    AS name,
    company_name::VARCHAR            AS company_name,
    company_gsid::VARCHAR            AS company_gsid,
    company_gsidd_09_dce::VARCHAR    AS company_gsidd_09_dce,
    tam_name::VARCHAR                AS tam_name,
    is_open::VARCHAR                 AS is_open,
    _fivetran_synced::TIMESTAMP      AS _fivetran_synced,
    company_gsid_8_adecc::VARCHAR    AS company_gsid_8_adecc,
    company_gside_6363_a::VARCHAR    AS company_gside_6363_a,
    company_gsid_62_d_58_a::VARCHAR  AS company_gsid_62_d_58_a,
    company_gsida_2_d_14_e::VARCHAR  AS company_gsida_2_d_14_e
  FROM source
)

SELECT *
FROM renamed
