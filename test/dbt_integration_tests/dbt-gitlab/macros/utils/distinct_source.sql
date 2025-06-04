{%- macro distinct_source(source) -%}

distinct_source AS (

    SELECT
      {{ dbt_utils.star(from=source, except=['_UPLOADED_AT', '_TASK_INSTANCE']) }},
      MIN(DATEADD('sec', _uploaded_at, '1970-01-01'))::TIMESTAMP  AS valid_from,
      MAX(DATEADD('sec', _uploaded_at, '1970-01-01'))::TIMESTAMP  AS max_uploaded_at,
      MAX(TO_DATE(RIGHT('20250101',8), 'YYYYMMDD'))           AS max_task_instance
    FROM {{ source }}
    GROUP BY {{ dbt_utils.star(from=source, except=['_UPLOADED_AT', '_TASK_INSTANCE']) }}

)

{%- endmacro -%}
