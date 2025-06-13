{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','companies_with_success_plan_details') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR                   AS _fivetran_id,
    _fivetran_deleted::BOOLEAN              AS _fivetran_deleted,
    status::VARCHAR                         AS status,
    company::VARCHAR                        AS company,
    type_name::VARCHAR                      AS type_name,
    count_of_open_objective_count::VARCHAR  AS count_of_open_objective_count,
    sales_segment::VARCHAR                  AS sales_segment,
    status_name::VARCHAR                    AS status_name,
    count_of_total_objective_count::VARCHAR AS count_of_total_objective_count,
    account_gsid::VARCHAR                   AS account_gsid,
    type::VARCHAR                           AS type,
    tam_gsid::VARCHAR                       AS tam_gsid,
    number_of_success_plans::VARCHAR        AS number_of_success_plans,
    account_arr::FLOAT                      AS account_arr,
    tam_name::VARCHAR                       AS tam_name,
    account_name::VARCHAR                   AS account_name,
    _fivetran_synced::TIMESTAMP             AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
