{%- macro channel_type(sqs_bucket_engagement, order_type) -%}

CASE
  WHEN {{ sqs_bucket_engagement }} = 'Partner Sourced'
    AND {{ order_type }} = '1. New - First Order'
    THEN 'Sourced - New'
  WHEN {{ sqs_bucket_engagement }} = 'Partner Sourced'
    AND ( {{ order_type }} != '1. New - First Order' OR {{ order_type }} IS NULL)
    THEN 'Sourced - Growth'
  WHEN {{ sqs_bucket_engagement }} = 'Co-sell'
    AND {{ order_type }} = '1. New - First Order'
    THEN 'Co-sell - New'
  WHEN {{ sqs_bucket_engagement }} = 'Co-sell'
    AND ( {{ order_type }} != '1. New - First Order' OR {{ order_type }} IS NULL)
    THEN 'Co-sell - Growth'
END

{%- endmacro -%}
