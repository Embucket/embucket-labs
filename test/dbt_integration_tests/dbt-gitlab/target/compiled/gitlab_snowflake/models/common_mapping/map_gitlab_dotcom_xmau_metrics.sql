

WITH gitlab_dotcom_xmau_metrics AS (

    SELECT * 
    FROM EMBUCKET.seed_product.gitlab_dotcom_xmau_metrics_common

), dotcom_event_to_edm_event AS (

    SELECT * 
    FROM EMBUCKET.seed_product.legacy_dotcom_event_name_to_edm_event_name_mapping

),

mapping_table AS (

    SELECT
      gitlab_dotcom_xmau_metrics.common_events_to_include,
      dotcom_event_to_edm_event.legacy_dotcom_event_name AS legacy_events_to_include,
      gitlab_dotcom_xmau_metrics.stage_name,
      gitlab_dotcom_xmau_metrics.smau,
      gitlab_dotcom_xmau_metrics.group_name,
      gitlab_dotcom_xmau_metrics.gmau,
      gitlab_dotcom_xmau_metrics.section_name,
      gitlab_dotcom_xmau_metrics.is_umau
    FROM gitlab_dotcom_xmau_metrics
    LEFT JOIN dotcom_event_to_edm_event
      ON gitlab_dotcom_xmau_metrics.common_events_to_include = dotcom_event_to_edm_event.prep_event_name

)

SELECT
      *,
      '@iweeks'::VARCHAR       AS created_by,
      '@iweeks'::VARCHAR       AS updated_by,
      '2022-04-09'::DATE        AS model_created_date,
      '2022-05-11'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM mapping_table