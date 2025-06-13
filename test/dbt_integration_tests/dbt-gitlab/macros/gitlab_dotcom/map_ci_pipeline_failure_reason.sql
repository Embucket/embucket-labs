{%- macro map_ci_pipeline_failure_reason(failure_reason_id) -%}

    CASE
      WHEN {{ failure_reason_id }}::NUMBER = 0 THEN 'unknown_failure'
      WHEN {{ failure_reason_id }}::NUMBER = 1 THEN 'config_error'
      WHEN {{ failure_reason_id }}::NUMBER = 2 THEN 'external_validation_failure'
      WHEN {{ failure_reason_id }}::NUMBER = 3 THEN 'user_not_verified'
      WHEN {{ failure_reason_id }}::NUMBER = 20 THEN 'activity_limit_exceeded'
      WHEN {{ failure_reason_id }}::NUMBER = 21 THEN 'size_limit_exceeded'
      WHEN {{ failure_reason_id }}::NUMBER = 22 THEN 'job_activity_limit_exceeded'
      WHEN {{ failure_reason_id }}::NUMBER = 23 THEN 'deployments_limit_exceeded'
      WHEN {{ failure_reason_id }}::NUMBER = 24 THEN 'user_blocked'
      WHEN {{ failure_reason_id }}::NUMBER = 25 THEN 'project_deleted'
      ELSE NULL
    END             

{%- endmacro -%}
