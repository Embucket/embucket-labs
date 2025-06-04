WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'marketing_dnc_list') }}

), renamed AS (

    SELECT
      address::VARCHAR                       AS email_address,
      is_role_address::BOOLEAN               AS is_role_address,
      is_disposable_address::BOOLEAN         AS is_disposable_address,
      did_you_mean::VARCHAR                  AS did_you_mean,
      result::VARCHAR                        AS result,
      reason::VARIANT                        AS reason,
      risk::VARCHAR                          AS risk,
      root_address::VARCHAR                  AS root_address
    FROM source
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY _updated_at DESC) = 1

)

SELECT *
FROM renamed
