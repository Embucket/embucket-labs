WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'order_action_rate_plan') }}

), renamed AS (

    SELECT
      "Id"::TEXT                                                           AS order_action_rate_plan_id,
      "OrderActionId"::TEXT                                                AS order_action_id,
      "RatePlanId"::TEXT                                                   AS rate_plan_id,
      "DELETED"::TEXT                                                      AS deleted,
      "CreatedById"::TEXT                                                  AS created_by_id,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP      AS created_date,
      "UpdatedById"::TEXT                                                  AS updated_by_id,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC',"UpdatedDate"))::TIMESTAMP       AS updated_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP               AS uploaded_at
    FROM source

)

SELECT *
FROM renamed
