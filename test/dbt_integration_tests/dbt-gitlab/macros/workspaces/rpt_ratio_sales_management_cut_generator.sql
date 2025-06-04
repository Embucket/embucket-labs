{% macro rpt_ratio_sales_management_cut_generator(select_columns, is_new_logo_calc, extra_where_clause='TRUE') %}

-- Metrics to compute
{%- set metrics = 
    ["MQLs / A",
    "MQLs / TQ",
    "MQLs / QTD",
    "Trials / A",
    "Trials / TQ",
    "Trials / QTD",
    "First Order ARR / A",
    "First Order ARR / TQ",
    "First Order ARR / QTD",
    "New Logos / A",
    "New Logos / TQ",
    "New Logos / QTD",
    "SAOs / A",
    "SAOs / TQ",
    "SAOs / QTD",
    "Won Opps / A",
    "Won Opps / TQ",
    "Won Opps / QTD",
    "Total Opps / A",
    "Total Opps / TQ",
    "Total Opps / QTD"]
  -%}

-- Regions inside Segment Region Grouped that will be part of Large Subtotal
{% set large_segment_region_grouped = ['APAC', 'EMEA', 'Large Other', 'PubSec', 'US East', 'US West', 'Pubsec', 'Global', 'Large MQLs & Trials'] %}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('rpt_crm_opportunity_closed_period', 'rpt_crm_opportunity_closed_period'),
    ('rpt_crm_opportunity_accepted_period', 'rpt_crm_opportunity_accepted_period'),
    ('rpt_crm_person_mql', 'rpt_crm_person_mql'),
    ('rpt_sales_funnel_target', 'rpt_sales_funnel_target'),
    ('rpt_sales_funnel_target_daily', 'rpt_sales_funnel_target_daily')
]) }}

-- For the rpt_models align the column names and how Missing values are encoded with the null_or_missing macro.
-- Also, add the variable extra_where_clause to filter the base data
, crm_person AS (

    SELECT
      {{ dbt_utils.star(from=ref('rpt_crm_person_mql'), except=['SALES_SEGMENT_GROUPED']) }},
      {{ null_or_missing('sales_segment_name', 'sales_segment') }},
      {{ null_or_missing('sales_segment_region_mapped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_person_mql
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{ extra_where_clause }}
  
), crm_opportunity_closed_period AS (

    SELECT
      rpt_crm_opportunity_closed_period.*,
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped', 'sales_segment') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_region_stamped_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped_grouped', 'sales_segment_grouped') }}
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
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
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
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
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
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
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
    
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_person
    
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
      COUNT(DISTINCT dim_crm_opportunity_id)               AS "New Logos / A"
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
  
), first_oder_arr_closed_won AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(net_arr)            AS "First Order ARR / A"
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      --AND order_type = '1. New - First Order'
      AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY ROLLUP(1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), win_rate AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(CASE WHEN is_won THEN 1 END)                            AS "Won Opps / A",
      COUNT(is_won)                                                 AS "Total Opps / A"
    FROM crm_opportunity_closed_period
    WHERE is_win_rate_calc = TRUE
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
   
), mql_trial_count AS (  
  
    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT email_hash)             AS "MQLs / A",
      COUNT(DISTINCT IFF(is_lead_source_trial, email_hash || lead_source, NULL)) AS "Trials / A"
    FROM crm_person
    GROUP BY ROLLUP( 1,
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
      SUM(IFF(kpi_name = 'Trials', qtd_allocated_target, 0)) AS  "Trials / QTD",
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  "New Logos / QTD",
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  "First Order ARR / QTD",
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', qtd_allocated_target, 0)) AS  "SAOs / QTD",
      SUM(IFF(kpi_name = 'MQL', qtd_allocated_target, 0)) AS  "MQLs / QTD",
      SUM(IFF(kpi_name = 'Deals', qtd_allocated_target, 0)) AS  "Won Opps / QTD",
      SUM(IFF(kpi_name = 'Total Closed', qtd_allocated_target, 0)) AS  "Total Opps / QTD"
    
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
      SUM(IFF(kpi_name = 'Trials', allocated_target, 0)) AS "Trials / TQ",
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS "New Logos / TQ",
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', allocated_target, 0)) AS "First Order ARR / TQ",
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', allocated_target, 0)) AS  "SAOs / TQ",
      SUM(IFF(kpi_name = 'MQL', allocated_target, 0)) AS  "MQLs / TQ",
      SUM(IFF(kpi_name = 'Deals', allocated_target, 0)) AS  "Won Opps / TQ",
      SUM(IFF(kpi_name = 'Total Closed', allocated_target, 0)) AS  "Total Opps / TQ"
    
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

    LEFT JOIN first_oder_arr_closed_won
      ON base_list.fiscal_quarter_name_fy = first_oder_arr_closed_won.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = first_oder_arr_closed_won.{{select_column}}
      {% endfor %}

    LEFT JOIN mql_trial_count
      ON base_list.fiscal_quarter_name_fy = mql_trial_count.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = mql_trial_count.{{select_column}}
      {% endfor %}

    LEFT JOIN sao_count
      ON base_list.fiscal_quarter_name_fy = sao_count.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = sao_count.{{select_column}}
      {% endfor %}

    LEFT JOIN win_rate
      ON base_list.fiscal_quarter_name_fy = win_rate.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
      AND base_list.{{select_column}} = win_rate.{{select_column}}
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
      "First Order ARR / A" IS NULL AND "First Order ARR / TQ" IS NULL
      AND "New Logos / A" IS NULL   AND "New Logos / TQ"       IS NULL
      AND "MQLs / A" IS NULL        AND "MQLs / TQ"            IS NULL
      AND "SAOs / A" IS NULL        AND "SAOs / TQ"            IS NULL
      AND "Won Opps / A" IS NULL    AND "Won Opps / TQ"        IS NULL
      AND "Total Opps / A" IS NULL  AND "Total Opps / TQ"      IS NULL
      AND "Trials / A" IS NULL      AND "Trials / TQ"          IS NULL
    )

)

-- The following code handles the large subtotal calculation
-- This is only calculated if the segment_region_grouped column is selected

-- In case the segment_region_grouped is the first column in the cut and there are other columns in the cut.
-- Additionally to Large-Total, a Large-Total | Total column (a subtotal for Large-Total) is calculated using the GROUP BY ROLLUP function.

{% for select_column in select_columns %}
  {% if select_column == 'segment_region_grouped' %}
  
    , large_subtotal_no_mql_trial_new_logo AS (

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
        COUNT(DISTINCT dim_crm_opportunity_id)               AS "New Logos / A"
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

    ), large_subtotal_mql_trial AS (

      SELECT
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total' AS {{column}},
          {% else %}
            IFNULL({{column}}, 'Total') AS {{column}},
          {% endif %}
        {% endfor %}
        COUNT(DISTINCT email_hash)             AS "MQLs / A",
        COUNT(DISTINCT IFF(is_lead_source_trial, email_hash || lead_source, NULL)) AS "Trials / A"
      FROM crm_person
      WHERE segment_region_grouped IN (
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
          large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy,
          {% for column in select_columns %}
            large_subtotal_no_mql_trial_new_logo.{{column}},
          {% endfor %}
          {% for metric in metrics %}
            {% if metric == 'MQLs / A' %}
              large_subtotal_mql_trial."{{metric}}" AS "{{metric}}"
            {% elif metric == 'Trials / A' %}
              large_subtotal_mql_trial."{{metric}}" AS "{{metric}}"
            {% elif metric == 'New Logos / A' %}
              large_subtotal_new_logo."{{metric}}" AS "{{metric}}"
            {% else %}
              large_subtotal_no_mql_trial_new_logo."{{metric}}"
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
        FROM large_subtotal_no_mql_trial_new_logo
        LEFT JOIN large_subtotal_mql_trial
          ON large_subtotal_mql_trial.fiscal_quarter_name_fy = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
          {% for select_column in select_columns %}
            {% if select_column == 'segment_region_grouped' %}
              AND IFF(large_subtotal_mql_trial.segment_region_grouped LIKE 'Large%', 'Large-Total', NULL) = large_subtotal_no_mql_trial_new_logo.segment_region_grouped
            {% else %}
              AND large_subtotal_mql_trial.{{select_column}} = large_subtotal_no_mql_trial_new_logo.{{select_column}}
            {% endif %}
          {% endfor %}
          AND large_subtotal_mql_trial.segment_region_grouped LIKE 'Large%'
        LEFT JOIN large_subtotal_new_logo
          ON large_subtotal_new_logo.fiscal_quarter_name_fy = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
          {% for select_column in select_columns %}
            AND large_subtotal_new_logo.{{select_column}} = large_subtotal_no_mql_trial_new_logo.{{select_column}}
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
  {% for select_column in select_columns %}
    {{select_column}},
  {% endfor %}
  "MQLs / A",
  "MQLs / QTD",
  "MQLs / A" / "MQLs / QTD"                   AS "MQLs / %QTD",
  "MQLs / TQ",
  "MQLs / A" / "MQLs / TQ"                    AS "MQLs / %TQ",

  "Trials / A",
  "Trials / QTD",
  "Trials / A" / "Trials / QTD"               AS "Trials / %QTD",
  "Trials / TQ",
  "Trials / A" / "Trials / TQ"                AS "Trials / %TQ",

  "First Order ARR / A",
   "New Logos / A",

  "First Order ARR / A" / "New Logos / A"     AS "ASP / A",
  "First Order ARR / TQ" / "New Logos / TQ"   AS "ASP / TQ",
  "ASP / A" / "ASP / TQ"                      AS "ASP / %TQ",

  "SAOs / A" / "MQLs / A"                     AS "MQLs to SAOs / A",
  "SAOs / TQ" / "MQLs / TQ"                   AS "MQLs to SAOs / TQ",
  "MQLs to SAOs / A" / "MQLs to SAOs / TQ"    AS "MQLs to SAOs / %TQ",

  "Won Opps / A" / "Total Opps / A"           AS "Win Rate / A",
  "Won Opps / TQ" / "Total Opps / TQ"         AS "Win Rate / TQ",
  "Win Rate / A" / "Win Rate / TQ"            AS "Win Rate / %TQ"

FROM final
WHERE fiscal_quarter_name_fy IS NOT NULL
  {% for select_column in select_columns %}
  AND {{select_column}} IS NOT NULL
  {% endfor %}

{%- endmacro -%}
