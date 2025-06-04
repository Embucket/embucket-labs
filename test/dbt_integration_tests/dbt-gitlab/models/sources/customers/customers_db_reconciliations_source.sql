WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_reconciliations') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id, order_id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT
      completed_email_sent_at::TIMESTAMP  AS completed_email_sent_at,
      created_at::TIMESTAMP               AS created_at,
      error_message::VARCHAR              AS error_message,
      id::NUMBER                          AS id,
      order_id::NUMBER                    AS order_id,
      qsr_opportunity_id::VARCHAR         AS qsr_opportunity_id,
      quoted_invoice_amount::NUMBER       AS quoted_invoice_amount,
      quoted_invoice_at::TIMESTAMP        AS quoted_invoice_at,
      reconcile_done_at::TIMESTAMP        AS reconcile_done_at,
      reconcile_on::TIMESTAMP             AS reconcile_on,
      skip_reason::VARCHAR                AS skip_reason,
      upcoming_email_sent_at::TIMESTAMP   AS upcoming_email_sent_at,
      updated_at::TIMESTAMP               AS updated_at,
      user_count::NUMBER                  AS user_count
    FROM source
    
)

SELECT *
FROM renamed