{%- macro sqs_bucket_engagement(column_1) -%}

IFF( {{ column_1 }} = 'Partner Generated', 'Partner Sourced', 'Co-sell')

{%- endmacro -%}