{%- macro monthly_all_time_metric_calc(metric_value, dim_installation_id, metrics_path, ping_created_at) -%}

{{ metric_value }} -
COALESCE(LAG({{ metric_value }}) OVER
        (PARTITION BY {{ dim_installation_id }}, {{ metrics_path }}
         ORDER BY {{ ping_created_at }} ASC), 0) AS monthly_metric_value

{%- endmacro -%}
