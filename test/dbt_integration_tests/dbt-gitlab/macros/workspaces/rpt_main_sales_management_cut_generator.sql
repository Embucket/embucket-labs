{% macro rpt_main_sales_management_cut_generator(select_columns, is_new_logo_calc, extra_where_clause='TRUE') %}

-- Metrics to compute
{%- set metrics = 
    ["Net ARR / A",
    "Net ARR / TQ",
    "Net ARR / QTD",
    "Logos / A",
    "Logos / TQ",
    "Logos / QTD",
    "Pipe / A",
    "Pipe / TQ",
    "Pipe / QTD",
    "SAOs / A",
    "SAOs / TQ",
    "SAOs / QTD"]
  -%}

-- Regions inside Segment Region Grouped that will be part of Large Subtotal
{% set large_segment_region_grouped = ['APAC', 'EMEA', 'Large Other', 'PubSec', 'US East', 'US West', 'Pubsec', 'Global', 'Large MQLs & Trials'] %}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('rpt_crm_opportunity_closed_period', 'rpt_crm_opportunity_closed_period'),
    ('rpt_crm_opportunity_accepted_period', 'rpt_crm_opportunity_accepted_period'),
    ('rpt_sales_funnel_target', 'rpt_sales_funnel_target'),
    ('rpt_sales_funnel_target_daily', 'rpt_sales_funnel_target_daily')
]) }}

-- For the rpt_models align the column names and how Missing values are encoded with the null_or_missing macro.
-- Also, add the variable extra_where_clause to filter the base data
, crm_opportunity_closed_period AS (

    SELECT
      rpt_crm_opportunity_closed_period.*,
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped', 'sales_segment') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_region_stamped_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped_grouped', 'sales_segment_grouped') }},
      CASE 
        WHEN crm_opp_owner_region_stamped = 'West'
          THEN 'US West'
        WHEN crm_opp_owner_region_stamped IN ('East', 'LATAM')
          THEN 'US East'
        WHEN crm_opp_owner_region_stamped IN ('APAC', 'PubSec','EMEA', 'Global')
          THEN crm_opp_owner_region_stamped
        WHEN crm_opp_owner_region_stamped NOT IN ('West', 'East', 'APAC', 'PubSec','EMEA', 'Global')
          THEN 'Other'
        ELSE 'Missing region_grouped'
      END AS region_grouped
    FROM rpt_crm_opportunity_closed_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), crm_opportunity_accepted_period AS (

    SELECT 
      rpt_crm_opportunity_accepted_period.*,
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }},
      CASE 
        WHEN crm_user_region = 'West'
          THEN 'US West'
        WHEN crm_user_region IN ('East', 'LATAM')
          THEN 'US East'
        WHEN crm_user_region IN ('APAC', 'PubSec','EMEA', 'Global')
          THEN crm_user_region
        WHEN crm_user_region IS NOT NULL
          THEN 'Other'
        ELSE 'Missing region_grouped'
      END AS region_grouped
    FROM rpt_crm_opportunity_accepted_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), sales_funnel_target AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }},
      CASE 
        WHEN crm_user_region = 'West'
          THEN 'US West'
        WHEN crm_user_region IN ('East', 'LATAM')
          THEN 'US East'
        WHEN crm_user_region IN ('APAC', 'PubSec','EMEA', 'Global')
          THEN crm_user_region
        WHEN crm_user_region IS NOT NULL
          THEN 'Other'
        ELSE 'Missing region_grouped'
      END AS region_grouped
    FROM rpt_sales_funnel_target
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), sales_funnel_target_daily AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target_daily'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }},
      CASE 
        WHEN crm_user_region = 'West'
          THEN 'US West'
        WHEN crm_user_region IN ('East', 'LATAM')
          THEN 'US East'
        WHEN crm_user_region IN ('APAC', 'PubSec','EMEA', 'Global')
          THEN crm_user_region
        WHEN crm_user_region IS NOT NULL
          THEN 'Other'
        ELSE 'Missing region_grouped'
      END AS region_grouped
    FROM rpt_sales_funnel_target_daily
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), current_fiscal_quarter AS (
  
    SELECT DISTINCT fiscal_quarter_name_fy AS current_fiscal_quarter
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

), factor_to_date AS (
  
    SELECT DISTINCT
      'date_range_quarter' AS _type,
      fiscal_quarter_name_fy::VARCHAR AS _date,
      IFF(fiscal_quarter_name_fy < current_fiscal_quarter.current_fiscal_quarter, TRUE, FALSE) AS is_selected_quarter_lower_than_current_quarter,
      last_day_of_fiscal_quarter
    FROM dim_date
    LEFT JOIN current_fiscal_quarter
    -- WHERE [fiscal_quarter_name_fy=bc_fiscal_quarter]
    WHERE fiscal_year BETWEEN EXTRACT(year FROM CURRENT_DATE) - 1 AND EXTRACT(year FROM CURRENT_DATE) + 1

)
-- Union all the data sources columns to create a base list that can be used to join all the metrics too
, prep_base_list AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_opportunity_closed_period
    
    UNION
    
    --SAOs
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_opportunity_accepted_period
    
    UNION
    
    -- Targets
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM sales_funnel_target
    
    UNION
    
    -- Targets MQL
    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM sales_funnel_target
    WHERE kpi_name IN ('MQL', 'Trials')
  
)
-- To the above base list add support for subtotals and a total column that will be joined later
, base_list AS (

    SELECT *
    FROM prep_base_list

    {% if select_columns|count > 1 %}

      {% for __ in select_columns %}

      UNION

      SELECT
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
          {% if loop.first %}
            {{select_column}}
          {% else %}
            'Total'
          {% endif %}
          {% if not loop.last %},{% endif %}

        {% endfor %}
      FROM prep_base_list

      {% endfor %}

    {% endif %}

    UNION

    SELECT
      fiscal_quarter_name_fy,
      {% for __ in select_columns %}
        'Total' {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM prep_base_list

)

-- Calculate the metrics in each CTE. Uses Group by ROLLUP to also calculate subtotal and total columns
-- The IFNULL in the select_column is used because the Subtotal and Total columns calculated by the rollup come back as NULL
, new_logos_actual AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT dim_crm_opportunity_id)               AS "Logos / A"
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
      AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY ROLLUP( 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )

), sao_count AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(*)                AS "SAOs / A"
    FROM crm_opportunity_accepted_period
    WHERE is_sao = TRUE
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), net_arr_actual AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(net_arr)            AS "Pipe / A"
    FROM crm_opportunity_closed_period
    WHERE is_net_arr_pipeline_created
    GROUP BY ROLLUP(1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), net_arr_closed_actual AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(net_arr)  AS "Net ARR / A"
    FROM crm_opportunity_closed_period
    WHERE is_net_arr_closed_deal = TRUE
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
   
), targets AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  "Logos / QTD",
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', qtd_allocated_target, 0)) AS  "SAOs / QTD",
      SUM(IFF(kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  "Net ARR / QTD",
      SUM(IFF(kpi_name = 'Net ARR Pipeline Created', qtd_allocated_target, 0)) AS  "Pipe / QTD"
    
    FROM sales_funnel_target_daily
    LEFT JOIN factor_to_date
    WHERE IFF(factor_to_date.is_selected_quarter_lower_than_current_quarter, target_date = factor_to_date.last_day_of_fiscal_quarter, report_target_date = CURRENT_DATE)
      --AND [fiscal_quarter_name_fy=bc_fiscal_quarter]
      AND fiscal_quarter_name_fy='FY22-Q1'
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), targets_full AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS  "Logos / TQ",
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', allocated_target, 0)) AS  "SAOs / TQ",
      SUM(IFF(kpi_name = 'Net ARR', allocated_target, 0)) AS  "Net ARR / TQ",
      SUM(IFF(kpi_name = 'Net ARR Pipeline Created', allocated_target, 0)) AS  "Pipe / TQ"
    
    FROM sales_funnel_target
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )

), agg AS (

    SELECT
      base_list.fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      base_list.{{select_column}},
      {% endfor %}

      {% for metric in metrics %}
      NULLIF("{{metric}}", 0)                   AS "{{metric}}"
      {% if not loop.last %},{% endif %}
      {% endfor %}

    FROM base_list

    INNER JOIN factor_to_date
      ON base_list.fiscal_quarter_name_fy::VARCHAR = factor_to_date._date::VARCHAR

    LEFT JOIN new_logos_actual
      ON base_list.fiscal_quarter_name_fy = new_logos_actual.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = new_logos_actual.{{select_column}}
      {% endfor %}

    LEFT JOIN net_arr_actual
      ON base_list.fiscal_quarter_name_fy = net_arr_actual.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = net_arr_actual.{{select_column}}
      {% endfor %}

    LEFT JOIN net_arr_closed_actual
      ON base_list.fiscal_quarter_name_fy = net_arr_closed_actual.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = net_arr_closed_actual.{{select_column}}
      {% endfor %}

    LEFT JOIN sao_count
      ON base_list.fiscal_quarter_name_fy = sao_count.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = sao_count.{{select_column}}
      {% endfor %}
    
    LEFT JOIN targets
      ON base_list.fiscal_quarter_name_fy = targets.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = targets.{{select_column}}
      {% endfor %}
    LEFT JOIN targets_full
      ON base_list.fiscal_quarter_name_fy = targets_full.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = targets_full.{{select_column}}
      {% endfor %}
    
    WHERE NOT (
      "Net ARR / A"   IS NULL   AND "Net ARR / TQ" IS NULL
      AND "Logos / A" IS NULL   AND "Logos / TQ"   IS NULL
      AND "Pipe / A"  IS NULL   AND "Pipe / TQ"    IS NULL
      AND "SAOs / A"  IS NULL   AND "SAOs / TQ"    IS NULL
    )

)

-- The following code handles the large subtotal calculation
-- This is only calculated if the segment_region_grouped column is selected

-- In case the segment_region_grouped is the first column in the cut and there are other columns in the cut.
-- Additionally to Large-Total, a Large-Total | Total column (a subtotal for Large-Total) is calculated using the GROUP BY ROLLUP function.

{% for select_column in select_columns %}
  {% if select_column == 'segment_region_grouped' %}
  
    , large_subtotal_no_new_logo AS (

        SELECT
          fiscal_quarter_name_fy,
          {% for column in select_columns %}
            {% if column == 'segment_region_grouped' %}
              'Large-Total' AS {{column}},
            {% else %}
              IFNULL({{column}}, 'Total') AS {{column}},
            {% endif %}
          {% endfor %}
          {% for metric in metrics %}
            SUM("{{metric}}") AS "{{metric}}"
            {% if not loop.last %},{% endif %}
          {% endfor %}
        FROM agg
        WHERE segment_region_grouped IN (
          {% for large_region in large_segment_region_grouped %} '{{large_region}}' {% if not loop.last %}, {% endif %} {% endfor %}
          )
          {% for select_column in select_columns %}
            AND agg.{{select_column}} != 'Total'
          {% endfor %}

        GROUP BY ROLLUP (
          1,
          {% for column in select_columns %}
            {% if column == 'segment_region_grouped' %}
              'Large-Total'
            {% else %}
              {{column}}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
        )

    ), large_subtotal_new_logo AS (

      SELECT
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total' AS {{column}},
          {% else %}
            IFNULL({{column}}, 'Total') AS {{column}},
          {% endif %}
        {% endfor %}
        COUNT(DISTINCT dim_crm_opportunity_id)               AS "Logos / A"
      FROM crm_opportunity_closed_period
      WHERE is_won = 'TRUE'
        AND is_closed = 'TRUE'
        AND is_edu_oss = 0
        -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
        AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
        AND segment_region_grouped IN (
          {% for large_region in large_segment_region_grouped %} '{{large_region}}' {% if not loop.last %}, {% endif %} {% endfor %}
        )

        GROUP BY ROLLUP(
          1,
          {% for column in select_columns %}
            {% if column == 'segment_region_grouped' %}
              'Large-Total'
            {% else %}
              {{column}}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
        )

    ), large_subtotal AS (

        SELECT
          large_subtotal_no_new_logo.fiscal_quarter_name_fy,
          {% for column in select_columns %}
            large_subtotal_no_new_logo.{{column}},
          {% endfor %}
          {% for metric in metrics %}
            {% if metric == 'Logos / A' %}
              large_subtotal_new_logo."{{metric}}" AS "{{metric}}"
            {% else %}
              large_subtotal_no_new_logo."{{metric}}"
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
        FROM large_subtotal_no_new_logo
        LEFT JOIN large_subtotal_new_logo
          ON large_subtotal_new_logo.fiscal_quarter_name_fy = large_subtotal_no_new_logo.fiscal_quarter_name_fy
          {% for select_column in select_columns %}
            AND large_subtotal_new_logo.{{select_column}} = large_subtotal_no_new_logo.{{select_column}}
          {% endfor %}

    )
    {% endif %}
{% endfor %}


, final AS (

SELECT *
FROM agg

{% for select_column in select_columns %}
  {% if select_column == 'segment_region_grouped' %}

  UNION

  SELECT *
  FROM large_subtotal

  {% endif %}
{% endfor %}

)

SELECT
  fiscal_quarter_name_fy,
  "Net ARR / A",
  "Net ARR / QTD",
  "Net ARR / A" / "Net ARR / QTD" AS "Net ARR / %QTD",
  "Net ARR / A" / "Net ARR / TQ"  AS "Net ARR / %TQ",

  "Logos / A",
  "Logos / QTD",
  "Logos / A" / "Logos / QTD"     AS "Logos / %QTD",
  "Logos / TQ",
  "Logos / A" / "Logos / TQ"      AS "Logos / %TQ",
  
  "Pipe / A",
  "Pipe / QTD",
  "Pipe / A" / "Pipe / QTD"       AS "Pipe / %QTD",
  "Pipe / TQ",
  "Pipe / A" / "Pipe / TQ"        AS "Pipe / %TQ",
  
  "SAOs / A",
  "SAOs / QTD",
  "SAOs / A" / "SAOs / QTD"       AS "SAOs / %QTD",
  "SAOs / TQ",
  "SAOs / A" / "SAOs / TQ"        AS "SAOs / %TQ"
 
FROM final
WHERE fiscal_quarter_name_fy IS NOT NULL
  {% for select_column in select_columns %}
  AND {{select_column}} IS NOT NULL
  {% endfor %}

{%- endmacro -%}
