WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'budget') }}

), renamed AS (

    SELECT
      --Primary Key
      budget_id::FLOAT             AS budget_id,

      --Foreign Keys
      accounting_period_id::FLOAT  AS accounting_period_id,
      account_id::FLOAT            AS account_id,
      department_id::FLOAT         AS department_id,
      subsidiary_id::FLOAT         AS subsidiary_id,
      category_id::FLOAT           AS category_id,

      --Info
      amount::FLOAT                AS budget_amount,
      _fivetran_deleted::BOOLEAN   AS is_fivetran_deleted

    FROM source

)

SELECT *
FROM renamed
