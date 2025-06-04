WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'refund') }}

), renamed AS (

    SELECT
      refund_number::VARCHAR                      AS refund_number,
      id::VARCHAR                                 AS refund_id,

      --Foreign Keys
      account_id::VARCHAR                         AS account_id,
      --parent_account_id::VARCHAR                  AS parent_account_id,

      --Info
      accounting_code::VARCHAR                    AS accounting_code,
      amount::FLOAT                               AS refund_amount,
      bill_to_contact_id::VARCHAR                 AS bill_to_contact_id,
      comment::VARCHAR                            AS comment,
      created_by_id::VARCHAR                      AS created_by_id,
      created_date::TIMESTAMP_TZ                  AS created_date,
      default_payment_method_id::VARCHAR          AS default_payment_method_id,
      gateway::VARCHAR                            AS gateway,
      gateway_response::VARCHAR                   AS gateway_response,
      gateway_response_code::VARCHAR              AS gateway_response_code,
      gateway_state::VARCHAR                      AS gateway_state,
      method_type::VARCHAR                        AS method_type,
      payment_method_id::VARCHAR                  AS payment_method_id,
      payment_method_snapshot_id::VARCHAR         AS payment_method_snapshot_id,
      reason_code::VARCHAR                        AS reason_code,
      reference_id::VARCHAR                       AS reference_id,
      refund_date::TIMESTAMP_TZ                   AS refund_date,
      refund_transaction_time::TIMESTAMP_TZ       AS refund_transaction_time,
      second_refund_reference_id::VARCHAR         AS second_refund_reference_id,
      soft_descriptor::VARCHAR                    AS soft_descriptor,
      soft_descriptor_phone::VARCHAR              AS soft_descriptor_phone,
      sold_to_contact_id::VARCHAR                 AS sold_to_contact_id,
      source_type::VARCHAR                        AS source_type,
      status::VARCHAR                             AS refund_status,
      submitted_on::TIMESTAMP_TZ                  AS submitted_on,
      transferred_to_accounting::VARCHAR          AS transferred_to_accounting,
      type::VARCHAR                               AS refund_type,
      updated_by_id::VARCHAR                      AS updated_by_id,
      updated_date::TIMESTAMP_TZ                  AS updated_date,
      _FIVETRAN_DELETED::BOOLEAN                  AS is_deleted

    FROM source

)

SELECT *
FROM renamed
