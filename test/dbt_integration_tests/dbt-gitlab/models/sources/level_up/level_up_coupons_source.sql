{{ level_up_intermediate('coupons') }}

parsed AS (
  SELECT
    value['active']::BOOLEAN              AS is_active,
    value['allowMultipleInCart']::BOOLEAN AS allow_multiple_in_cart,
    value['amountOffInCents']::INT        AS amount_off_in_cents,
    value['appliesToUser']::VARCHAR       AS applies_to_user,
    value['code']::VARCHAR                AS coupon_code,
    value['id']::VARCHAR                  AS coupon_id,
    value['maxRedemptions']::INT          AS max_redemptions,
    value['percentOff']::INT              AS percent_off,
    value['redeemBy']::TIMESTAMP          AS redeem_by,
    value['timesRedeemed']::INT           AS times_redeemed,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        coupon_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
