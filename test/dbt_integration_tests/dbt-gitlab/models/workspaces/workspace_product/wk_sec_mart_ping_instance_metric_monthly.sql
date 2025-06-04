 {{ config({    
    "materialized":"view",
    "tags":["mnpi_exception","product"]
}) }}

WITH dim_ping_metric AS (

  SELECT 
    {{ dbt_utils.star(
        from=ref('dim_ping_metric'), 
        except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
        ) 
    }}
  FROM {{ ref('dim_ping_metric') }}
  WHERE (section_name = 'sec' OR is_umau = TRUE) --Only include Sec metrics or the UMAU metric (for event adoption calculations)
  ORDER BY metrics_path ASC

), sec_ping_metrics AS (

  SELECT 
    {{ dbt_utils.star(
        from=ref('mart_ping_instance_metric_monthly'), 
        except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
        ) 
    }}
  FROM {{ ref('mart_ping_instance_metric_monthly') }}
  WHERE (section_name = 'sec' OR is_umau = TRUE) --Only include Sec metrics or the UMAU metric (for event adoption calculations)
    AND metric_value > 0 --Filter to exclude events that have never received usage from the installation.
  ORDER BY ping_created_date_month DESC

), results AS (

  SELECT
    sec_ping_metrics.*,
    dim_ping_metric.description
  FROM sec_ping_metrics
  INNER JOIN dim_ping_metric ON sec_ping_metrics.metrics_path = dim_ping_metric.metrics_path
  
)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dpeterson",
    updated_by="@dpeterson",
    created_date="2024-04-04",
    updated_date="2024-04-04"
) }}
