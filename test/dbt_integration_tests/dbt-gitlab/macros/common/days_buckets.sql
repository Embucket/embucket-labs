{%- macro days_buckets(day_field) -%}

CASE 
  WHEN {{ day_field }} BETWEEN 0 AND 6
    THEN '[01] 0-6 Days'
  WHEN {{ day_field }} BETWEEN 7 AND 14
    THEN '[02] 7-14 Days'
  WHEN {{ day_field }} BETWEEN 15 AND 21
    THEN '[03] 15-21 Days'
  WHEN {{ day_field }} BETWEEN 22 AND 30
    THEN '[04] 22-30 Days'
  WHEN {{ day_field }} BETWEEN 31 AND 60
    THEN '[05] 31-60 Days'
  WHEN {{ day_field }} BETWEEN 61 AND 90
    THEN '[06] 61-90 Days'
  WHEN {{ day_field }} BETWEEN 91 AND 120
    THEN '[07] 91-120 Days'
  WHEN {{ day_field }} BETWEEN 121 AND 180
    THEN '[08] 121-180 Days'
  WHEN {{ day_field }} BETWEEN 181 AND 365
    THEN '[09] 181-365 Days'
  WHEN {{ day_field }} BETWEEN 366 AND 730
    THEN '[10] 1-2 Years'
  WHEN {{ day_field }} BETWEEN 731 AND 1095
    THEN '[11] 2-3 Years'
  WHEN {{ day_field }} > 1095
    THEN '[12] 3+ Years'
END

{%- endmacro -%}
