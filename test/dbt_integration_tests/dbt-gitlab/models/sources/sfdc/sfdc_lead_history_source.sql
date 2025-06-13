WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'lead_history') }}

), renamed AS (

    SELECT
      id                    AS lead_history_id,
      leadid                AS lead_id,
      LOWER(field)          AS lead_field,
      newvalue__fl          AS new_value_float,
      newvalue__de          AS new_value_decimal,
      newvalue__st          AS new_value_string,
      newvalue__bo          AS new_value_boolean,
      oldvalue__fl          AS old_value_float,
      oldvalue__de          AS old_value_decimal,
      oldvalue__st          AS old_value_string,
      oldvalue__bo          AS old_value_boolean,
      --metadata
      isdeleted             AS is_deleted,
      createdbyid           AS created_by_id,
      createddate           AS field_modified_at
    FROM base

)

SELECT *
FROM renamed
