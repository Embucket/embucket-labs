{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','ctas_healthscores') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR            AS _fivetran_id,
    _fivetran_deleted::BOOLEAN       AS _fivetran_deleted,
    stage_name::VARCHAR              AS stage_name,
    objective_category_name::VARCHAR AS objective_category_name,
    cta_status_name::VARCHAR         AS cta_status_name,
    cta_company_id::VARCHAR          AS cta_company_id,
    trend::VARCHAR                   AS trend,
    cta_reason::VARCHAR              AS cta_reason,
    playbook_name::VARCHAR           AS playbook_name,
    reason::VARCHAR                  AS reason,
    objective_category_id::VARCHAR   AS objective_category_id,
    current_score::VARCHAR           AS current_score,
    company_name::VARCHAR            AS company_name,
    type::VARCHAR                    AS type,
    cta_name::VARCHAR                AS cta_name,
    previous_score::VARCHAR          AS previous_score,
    measure::VARCHAR                 AS measure,
    cta_type::VARCHAR                AS cta_type,
    cta_status_id::VARCHAR           AS cta_status_id,
    closed_date::TIMESTAMP           AS closed_date,
    _fivetran_synced::TIMESTAMP      AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
