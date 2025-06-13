WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'sla_policies') }}

), renamed AS (

  SELECT
    id::VARCHAR                                      AS zendesk_sla_policy_id,
    title::VARCHAR                                   AS zendesk_sla_title,
    description::VARCHAR                             AS zendesk_sla_description,
    filter_all.value['field']::VARCHAR               AS filter_all_field,
    filter_all.value['operator']::VARCHAR            AS filter_all_operator,
    filter_all.value['value']::VARCHAR               AS filter_all_value,
    filter_any.value['field']::VARCHAR               AS filter_any_field,
    filter_any.value['operator']::VARCHAR            AS filter_any_operator,
    filter_any.value['value']::VARCHAR               AS filter_any_value,
    policy_metrics.value['business_hours']::VARCHAR  AS policy_metrics_business_hours,
    policy_metrics.value['metric']::VARCHAR          AS policy_metrics_metric,
    policy_metrics.value['priority']::VARCHAR        AS policy_metrics_priority,
    policy_metrics.value['target']::VARCHAR          AS policy_metrics_target
  FROM source,
    LATERAL FLATTEN(INPUT => parse_json(filter), path => 'all', outer => true) filter_all,
    LATERAL FLATTEN(INPUT => parse_json(filter), path => 'any', outer => true) filter_any,
    LATERAL FLATTEN(INPUT => parse_json(policy_metrics), outer => true) policy_metrics

), keyed AS (

  SELECT {{ dbt_utils.generate_surrogate_key(['zendesk_sla_policy_id', 'filter_all_field', 'filter_all_operator',
            'filter_all_value', 'filter_any_field', 'filter_any_operator', 'filter_any_value',
          'policy_metrics_business_hours', 'policy_metrics_metric', 'policy_metrics_priority', 'policy_metrics_target']) }} AS zendesk_sla_surrogate_key,
        *
  FROM renamed

)

SELECT *
FROM keyed
