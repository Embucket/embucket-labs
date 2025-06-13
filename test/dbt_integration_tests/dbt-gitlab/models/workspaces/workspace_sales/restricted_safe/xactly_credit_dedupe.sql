WITH source AS (

  SELECT {{ hash_sensitive_columns('xactly_credit_source') }}
  FROM {{ ref('xactly_credit_source') }}

),

deduped_source AS (

  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY
        order_code, item_code, participant_id, credit_type_id, rule_id
      ORDER BY modified_date DESC
    ) AS row_number
  FROM source

)

SELECT *
FROM deduped_source
WHERE row_number = 1
