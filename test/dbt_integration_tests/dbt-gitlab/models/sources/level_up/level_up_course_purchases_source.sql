{{ level_up_intermediate('course_purchases') }}

parsed AS (
  SELECT
    value['id']::VARCHAR                        AS course_purchase_id,
    value['transactionId']::VARCHAR             AS transaction_id,
    {{ level_up_filter_gitlab_email("value['giftRecipientEmail']") }} AS gift_recipient_email,
    value['timestamp']::TIMESTAMP               AS event_timestamp,
    value['companyId']::VARCHAR                 AS company_id,
    value['coupon']::VARIANT                    AS coupon,
    value['couponCode']::VARCHAR                AS coupon_code,
    value['purchasableId']::VARCHAR             AS purchasable_id,
    value['purchasableType']::VARCHAR           AS purchasable_type,
    value['chargeId']::VARCHAR                  AS charge_id,
    value['groupId']::VARCHAR                   AS group_id,
    value['orderId']::VARCHAR                   AS order_id,
    value['revenueType']::VARCHAR               AS revenue_type,
    value['quantity']::INT                      AS quantity,
    value['variation']::VARCHAR                 AS variation,
    value['shippingMethod']::VARCHAR            AS shipping_method,
    value['type']::VARCHAR                      AS transaction_type,
    value['sku']::VARCHAR                       AS course_sku,
    value['slug']::VARCHAR                      AS course_slug,
    value['title']::VARCHAR                     AS course_title,
    value['courseStartDate']::TIMESTAMP         AS course_start_date,
    value['success']::BOOLEAN                   AS is_success,
    value['failureCode']::VARCHAR               AS failure_code,
    value['failureMessage']::VARCHAR            AS failure_message,

    value['userDetail']['id']::VARCHAR          AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,

    value['referral']['referrer']::VARCHAR      AS referral_referrer,
    value['referral']['referrer_type']::VARCHAR AS referral_referrer_type,
    value['referral']['source']::VARCHAR        AS referral_source,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        course_purchase_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
