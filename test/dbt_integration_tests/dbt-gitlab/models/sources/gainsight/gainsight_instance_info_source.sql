{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}


WITH source AS (

    SELECT *
    FROM {{ source('gainsight', 'gainsight_instance_info') }}

), final AS (

    SELECT 
      crm_acct_id::VARCHAR                                 AS crm_account_id,
      gainsight_unique_row_id::VARCHAR                     AS gainsight_unique_row_id,
      instance_uuid::VARCHAR                               AS instance_uuid,
      hostname::VARCHAR                                    AS instance_hostname,
      instancetype::VARCHAR                                AS instance_type,
      "Namespace_ID"::VARCHAR                              AS namespace_id,
      included_in_health_measures::VARCHAR                 AS included_in_health_measures_str,
      to_timestamp(_updated_at::NUMBER)                    AS uploaded_at
    FROM source

)

SELECT * 
FROM final
