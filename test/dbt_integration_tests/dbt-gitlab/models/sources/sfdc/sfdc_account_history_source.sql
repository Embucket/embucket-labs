WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'account_history') }}

), renamed AS (

    SELECT
      accountid                     AS account_id,
      id                            AS account_history_id,
      createddate                   AS field_modified_at,
      LOWER(field)                  AS account_field,
      newvalue__fl                  AS new_value_float,
      newvalue__st                  AS new_value_string,
      newvalue__bo                  AS new_value_boolean,
      oldvalue__fl                  AS old_value_float,
      oldvalue__st                  AS old_value_string,
      oldvalue__bo                  AS old_value_boolean,
      COALESCE(
        newvalue__fl::VARCHAR,
        newvalue__st::VARCHAR,
        newvalue__bo::VARCHAR
      )                             AS new_value,
      COALESCE(
        oldvalue__fl::VARCHAR,
        oldvalue__st::VARCHAR,
        oldvalue__bo::VARCHAR
      )                             AS old_value,
      isdeleted                     AS is_deleted,
      createdbyid                   AS created_by_id
    FROM base  

)

SELECT *
FROM renamed
