-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'zuora_refund_snapshots') }}

), renamed AS (

    SELECT
      --Primary Keys
      refundnumber::VARCHAR                  AS refund_number,
      id::VARCHAR                            AS refund_id,

      --Foreign Keys
      accountid::VARCHAR                     AS account_id,
      parentaccountid::VARCHAR               AS parent_account_id,

      --Info
      accountingcode::VARCHAR                AS accounting_code,
      amount::FLOAT                          AS refund_amount,
      billtocontactid::VARCHAR               AS bill_to_contact_id,
      cancelledon::TIMESTAMP_TZ              AS cancelled_on,
      comment::VARCHAR                       AS comment,
      createdbyid::VARCHAR                   AS created_by_id,
      createddate::TIMESTAMP_TZ              AS created_date,
      defaultpaymentmethodid::VARCHAR        AS default_payment_method_id,
      gateway::VARCHAR                       AS gateway,
      gatewayresponse::VARCHAR               AS gateway_response,
      gatewayresponsecode::VARCHAR           AS gateway_response_code,
      gatewaystate::VARCHAR                  AS gateway_state,
      methodtype::VARCHAR                    AS method_type,
      paymentmethodid::VARCHAR               AS payment_method_id,
      paymentmethodsnapshotid::VARCHAR       AS payment_method_snapshot_id,
      reasoncode::VARCHAR                    AS reason_code,
      referenceid::VARCHAR                   AS reference_id,
      refunddate::TIMESTAMP_TZ               AS refund_date,
      refundtransactiontime::TIMESTAMP_TZ    AS refund_transaction_time,
      secondrefundreferenceid::VARCHAR       AS second_refund_reference_id,
      softdescriptor::VARCHAR                AS soft_descriptor,
      softdescriptorphone::VARCHAR           AS soft_descriptor_phone,
      soldtocontactid::VARCHAR               AS sold_to_contact_id,
      sourcetype::VARCHAR                    AS source_type,
      status::VARCHAR                        AS refund_status,
      submittedon::TIMESTAMP_TZ              AS submitted_on,
      transferredtoaccounting::VARCHAR       AS transferred_to_accounting,
      type::VARCHAR                          AS refund_type,
      updatedbyid::VARCHAR                   AS updated_by_id,
      updateddate::TIMESTAMP_TZ              AS updated_date,
      deleted::BOOLEAN                       AS is_deleted,

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to

    FROM source

)

SELECT *
FROM renamed
