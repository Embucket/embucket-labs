{{ simple_cte([
    ('gitlab_dotcom_xmau_metrics', 'gitlab_dotcom_xmau_metrics_common'),
    ('dotcom_event_to_edm_event', 'legacy_dotcom_event_name_to_edm_event_name_mapping')
]) }},

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

{{ dbt_audit(
    cte_ref="mapping_table",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-05-11"
) }}
