WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'users') }}

), renamed AS (

    SELECT
      "Id"::TEXT                                                           AS zuora_user_id,
      "Email"::TEXT                                                        AS email,
      "FirstName"::TEXT                                                    AS first_name,
      "LastName"::TEXT                                                     AS last_name,
      "Username"::TEXT                                                     AS user_name,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP      AS created_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP               AS uploaded_at
    FROM source

)

SELECT *
FROM renamed
