WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'revenue_schedule_item') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                           AS revenue_schedule_item_id,

      --Foreign Keys
      accountid::VARCHAR                    AS account_id,
      parentaccountid::VARCHAR              AS parent_account_id,
      accountingperiodid::VARCHAR           AS accounting_period_id,
      amendmentid::VARCHAR                  AS amendment_id,
      subscriptionid::VARCHAR               AS subscription_id,
      productid::VARCHAR                    AS product_id,
      rateplanchargeid::VARCHAR             AS rate_plan_charge_id,
      rateplanid::VARCHAR                   AS rate_plan_id,
      soldtocontactid::VARCHAR              AS sold_to_contact_id,

      --Info
      amount::FLOAT                         AS revenue_schedule_item_amount,
      billtocontactid::VARCHAR              AS bill_to_contact_id,
      currency::VARCHAR                     AS currency,
      createdbyid::VARCHAR                  AS created_by_id,
      createddate::TIMESTAMP_TZ             AS created_date,
      defaultpaymentmethodid::VARCHAR       AS default_payment_method_id,
      deleted::BOOLEAN                      AS is_deleted,
      updatedbyid::VARCHAR                  AS updated_by_id,
      updateddate::TIMESTAMP_TZ             AS updated_date

      FROM source

)

SELECT *
FROM renamed
