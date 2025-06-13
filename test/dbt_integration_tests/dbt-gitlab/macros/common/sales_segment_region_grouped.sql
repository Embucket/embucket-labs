{%- macro sales_segment_region_grouped(segment, sales_geo, sales_region) -%}

CASE 
  WHEN UPPER({{ segment }}) IN ('LARGE', 'PUBSEC') AND UPPER({{ sales_geo }}) = 'AMER' AND UPPER({{ sales_region }}) = 'WEST'
    THEN 'US WEST'
  WHEN UPPER({{ segment }}) IN ('LARGE', 'PUBSEC') AND UPPER({{ sales_geo }}) IN ('AMER', 'LATAM') AND UPPER({{ sales_region }}) IN ('EAST', 'LATAM')
    THEN 'US EAST'
  WHEN UPPER({{ segment }}) IN ('LARGE', 'PUBSEC') AND UPPER({{ sales_geo }}) IN ('APAC', 'PUBSEC','EMEA', 'GLOBAL')
    THEN {{ sales_geo }}
  WHEN UPPER({{ segment }}) IN ('LARGE', 'PUBSEC') AND UPPER({{ sales_region }}) = 'PUBSEC'
    THEN 'PUBSEC'
  WHEN UPPER({{ segment }}) IN ('LARGE', 'PUBSEC') AND UPPER({{ sales_geo }}) NOT IN ('WEST', 'EAST', 'APAC', 'PUBSEC','EMEA', 'GLOBAL')
    THEN 'LARGE OTHER'
  WHEN UPPER({{ segment }}) NOT IN ('LARGE', 'PUBSEC')
    THEN {{ segment }}
  ELSE 'Missing segment_region_grouped'
END

{%- endmacro -%}
