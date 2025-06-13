WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'rampinterval') }}

), renamed AS (

    SELECT

      "Id"::TEXT                                                             AS ramp_interval_id,
      "Rampid"::TEXT                                                         AS ramp_id,
      "StartDate"::DATE                                                      AS start_date,
      "EndDate"::DATE                                                        AS end_date,
      "Name"::TEXT                                                           AS name,
      "Description"::TEXT                                                    AS description,
      "GrossTcb"::NUMBER                                                     AS gross_tcb,
      "DiscountTcv"::NUMBER                                                  AS discount_tcv,
      "NetTcb"::NUMBER                                                       AS net_tcb,
      "GrossTcv"::NUMBER                                                     AS gross_tcv,
      "NetTcv"::NUMBER                                                       AS net_tcv,
      "DiscountTcb"::NUMBER                                                  AS discount_tcb,
      "CreatedById"::TEXT                                                    AS created_by_id,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP        AS created_date,
      "UpdatedById"::TEXT                                                    AS updated_by_id,
      "UpdatedDate"::DATETIME                                                AS updated_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP                 AS uploaded_at

    FROM source

)

SELECT *
FROM renamed
