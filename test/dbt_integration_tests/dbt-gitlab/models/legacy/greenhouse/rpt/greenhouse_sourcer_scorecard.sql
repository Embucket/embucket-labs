WITH  greenhouse_metrics_unpivoted AS (
    
     {{ dbt_utils.unpivot(relation=ref('greenhouse_sourcer_base'), cast_to='FLOAT', 
            exclude=['reporting_month', 'start_period','end_period','sourcer_name','month_date'])
     }}

), sourcer_metrics AS (

    {{ dbt_utils.unpivot(relation=ref('greenhouse_sourcer_metrics'), cast_to='FLOAT', 
                exclude=['month_date','sourcer_name','part_of_recruiting_team'])
        }}

), outlier AS (

    SELECT 
      reporting_month,
      field_name,
      PERCENTILE_CONT(0.1) WITHIN group (ORDER BY VALUE)  AS outlier
    FROM greenhouse_metrics_unpivoted
    WHERE VALUE IS NOT NULL
    GROUP BY 1,2

), baseline AS (

    SELECT
      greenhouse_metrics_unpivoted.reporting_month,
      greenhouse_metrics_unpivoted.field_name,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY VALUE)  AS percentile_25th,
      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY VALUE)  AS percentile_50th,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY VALUE)  AS percentile_75th,
      PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY VALUE)  AS percentile_80th,
      PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY VALUE)  AS ninetieth_percentile,
      PERCENTILE_CONT(1.00) WITHIN GROUP (ORDER BY VALUE)  AS percentile_max 
    FROM greenhouse_metrics_unpivoted
    LEFT JOIN outlier
      ON greenhouse_metrics_unpivoted.reporting_month = outlier.reporting_month
      AND greenhouse_metrics_unpivoted.field_name = outlier.field_name
    WHERE greenhouse_metrics_unpivoted.VALUE > outlier.outlier
    ----removing outliers when identifying the percentiles to use---
    GROUP BY 1,2

), final AS (

    SELECT 
      sourcer_metrics.month_date,
      sourcer_metrics.sourcer_name,
      sourcer_metrics.field_name        AS recruiting_metric,
      sourcer_metrics.value             AS recruiting_metric_value,
      baseline.percentile_25th,
      baseline.percentile_50th,
      baseline.percentile_75th,
      baseline.percentile_80th,
      baseline.ninetieth_percentile,
      baseline.percentile_max
    FROM sourcer_metrics
    LEFT JOIN baseline 
      ON sourcer_metrics.month_date = baseline.reporting_month 
      AND sourcer_metrics.field_name = baseline.field_name

)

SELECT *
FROM final