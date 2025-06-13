{%- macro utm_content_parsing(utm_column_name) -%}

CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 2  
    THEN SPLIT_PART({{utm_column_name}} , '_', 1) 
END AS utm_content_offer,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 2  
    THEN SPLIT_PART({{utm_column_name}} , '_', 2) 
END AS utm_content_asset_type,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 2  
    THEN SPLIT_PART({{utm_column_name}} , '_', 3) 
END AS utm_content_industry,

{%- endmacro -%}