{{ simple_cte([
    ('usage_ping_metrics','sheetload_usage_ping_metrics_sections_source')
]) }}

, final AS (

    SELECT 
      section_name, 
      stage_name, 
      group_name, 
      metrics_path,
      'raw_usage_data_payload['''
        || REPLACE(metrics_path, '.', '''][''')
        || ''']'                                AS sql_friendly_path,                              
      clean_metrics_name, 
      periscope_metrics_name,
      REPLACE(periscope_metrics_name, '.', '_') AS sql_friendly_name,
      is_umau, 
      is_smau, 
      is_gmau, 
      is_paid_gmau,
      time_period 
    FROM usage_ping_metrics
    WHERE 
      is_smau = TRUE OR 
      is_gmau = TRUE OR 
      is_umau = TRUE OR 
      is_paid_gmau = TRUE 

) 

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@ischweickartDD",
    created_date="2021-03-01",
    updated_date="2021-03-15"
) }}
