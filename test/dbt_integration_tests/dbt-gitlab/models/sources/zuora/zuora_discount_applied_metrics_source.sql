-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom

WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'discount_applied_metrics') }}

), renamed AS(

    SELECT
      id                                      AS discount_applied_metrics_id, 
    
      --keys 
      accountid                               AS account_id,
      amendmentid                             AS amendment_id, 
      billtocontactid                         AS bill_to_contact_id, 
      defaultpaymentmethodid                  AS default_payment_method_id, 
      discountrateplanchargeid                AS discount_rate_plan_charge_id, 
      parentaccountid                         AS parent_account_id, 
      productid                               AS product_id, 
      productrateplanchargeid                 AS product_rate_plan_charge_id, 
      productrateplanid                       AS product_rate_plan_id, 
      rateplanchargeid                        AS rate_plan_charge_id, 
      rateplanid                              AS rate_plan_id,
      soldtocontactid                         AS sold_to_contact_id, 
      subscriptionid                          AS subscription_id, 
    
      --info 
      date_trunc('month', enddate)::DATE      AS end_date, 
      date_trunc('month', startdate)::DATE    AS start_date,
      mrr, 
      tcv,
    
      --metadata 
      createdbyid                             AS created_by_id, 
      createddate                             AS created_date,

      updatedbyid                             AS updated_by_id,
      updateddate                             AS updated_date
    FROM source

)

SELECT *
FROM renamed
