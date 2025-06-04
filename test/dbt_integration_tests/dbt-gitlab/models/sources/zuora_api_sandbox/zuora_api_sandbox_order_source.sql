WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', "order") }}

), renamed AS (

    SELECT

      id                                    AS dim_order_id,
      
      -- keys
      parentaccountid                       AS parent_account_id,
      billtocontactid                       AS bill_to_contact_id,
      soldtocontactid                       AS sold_to_contact_id,
      defaultpaymentmethodid                AS default_payment_method_id,

      -- account_info
      orderdate::DATE                       AS order_date,
      ordernumber                           AS order_number,
      description                           AS order_description,
      state                                 AS order_state,
      status                                AS order_status,
      createdbymigration                    AS is_created_by_migration,
      
      -- metadata
      createdbyid                           AS order_created_by_id,
      createddate::DATE                     AS order_created_date,
      deleted                               AS is_deleted,
      updatedbyid                           AS update_by_id,
      updateddate::DATE                     AS updated_date

    FROM source

)

SELECT *
FROM renamed
