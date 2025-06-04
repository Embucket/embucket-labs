{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'opportunity_field_history') }}

), renamed AS (

    SELECT
      opportunityid         AS opportunity_id,
      id                    AS field_history_id,
      createddate           AS field_modified_at,
      LOWER(field)          AS opportunity_field,
      newvalue__fl          AS new_value_float,
      newvalue              AS new_value_string,
      newvalue__bo          AS new_value_boolean,
      newvalue__de          AS new_value_decimal,
      oldvalue__fl          AS old_value_float,
      oldvalue              AS old_value_string,
      oldvalue__bo          AS old_value_boolean,
      oldvalue__de          AS old_value_decimal,
      COALESCE(
        newvalue__fl::VARCHAR,
        newvalue::VARCHAR,
        newvalue__bo::VARCHAR,
        newvalue__de::VARCHAR
      )                     AS new_value,
      COALESCE(
        oldvalue__fl::VARCHAR,
        oldvalue::VARCHAR,
        oldvalue__bo::VARCHAR,
        oldvalue__de::VARCHAR
      )                     AS old_value,
      isdeleted             AS is_deleted,
      createdbyid           AS created_by_id
    FROM base

)

SELECT *
FROM renamed
