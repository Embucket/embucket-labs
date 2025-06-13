{{ config({
    "alias": "customers_db_customers_snapshots"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'customers_db_customers_snapshots') }}

), renamed AS (

  SELECT
    dbt_scd_id::VARCHAR              AS customer_snapshot_id,
    id::NUMBER                       AS customer_id,
    created_at::TIMESTAMP            AS customer_created_at,
    updated_at::TIMESTAMP            AS customer_updated_at,
    --sign_in_count // missing from manifest, issue 1860,
    current_sign_in_at::TIMESTAMP    AS current_sign_in_at,
    last_sign_in_at::TIMESTAMP       AS last_sign_in_at,
    --current_sign_in_ip,
    --last_sign_in_ip,
    provider::VARCHAR                AS customer_provider,
    NULLIF(uid, '')::VARCHAR         AS customer_uid,
    country::VARCHAR                 AS country,
    state::VARCHAR                   AS state,
    city::VARCHAR                    AS city,
    company::VARCHAR                 AS company,
    salesforce_account_id::VARCHAR   AS sfdc_account_id,
    billable::BOOLEAN                AS customer_is_billable,
    access_token::VARCHAR            AS access_token,
    confirmation_token::VARCHAR      AS confirmation_token,
    confirmed_at::TIMESTAMP          AS confirmed_at,
    confirmation_sent_at::TIMESTAMP  AS confirmation_sent_at,
    "DBT_VALID_FROM"::TIMESTAMP      AS valid_from,
    "DBT_VALID_TO"::TIMESTAMP        AS valid_to

  FROM source

)

SELECT *
FROM renamed
