{%- macro utm_campaign_parsing(utm_column_name) -%}

CASE 
    WHEN REGEXP_COUNT(utm_campaign, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART(utm_campaign , '_', 1) 
END AS utm_campaign_date,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART({{utm_column_name}} , '_', 2) 
END AS utm_campaign_region,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART({{utm_column_name}} , '_', 3) 
END AS utm_campaign_budget,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART({{utm_column_name}} , '_', 4) 
END AS utm_campaign_type,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART({{utm_column_name}} , '_', 5) 
END AS utm_campaign_gtm,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_')
    THEN SPLIT_PART({{utm_column_name}} , '_', 6) 
END AS utm_campaign_language,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 
        AND (REGEXP_LIKE(utm_campaign, '.*(20[0-9]{6}).*') OR LEFT(utm_campaign, 3) = 'eg_') 
        AND LEFT(SPLIT_PART(utm_campaign , '_', 8), 2) = 'a-'
    THEN RIGHT(SPLIT_PART(utm_campaign , '_', 8), LENGTH(SPLIT_PART(utm_campaign , '_', 8)) - 2)
END AS utm_campaign_agency,
CASE 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 
        AND (REGEXP_LIKE({{utm_column_name}}, '.*(20[0-9]{6}).*') OR LEFT({{utm_column_name}}, 3) = 'eg_')
        AND LEFT(SPLIT_PART({{utm_column_name}} , '_', 8), 2) = 'a-'
        THEN SUBSTRING(
            RIGHT({{utm_column_name}}, LEN({{utm_column_name}}) - regexp_instr({{utm_column_name}},
            '_', 1, 6)),
            1,
            CHARINDEX('_a-', RIGHT({{utm_column_name}}, LEN({{utm_column_name}}) - regexp_instr({{utm_column_name}}, '_', 1, 6))) - 1
            ) 
    WHEN REGEXP_COUNT({{utm_column_name}}, '_') >= 6 
        AND (REGEXP_LIKE({{utm_column_name}}, '.*(20[0-9]{6}).*') OR LEFT({{utm_column_name}}, 3) = 'eg_')
        THEN RIGHT({{utm_column_name}} , LEN({{utm_column_name}} ) - regexp_instr({{utm_column_name}} ,'_',1,6))
END AS utm_campaign_name,
{%- endmacro -%}
    