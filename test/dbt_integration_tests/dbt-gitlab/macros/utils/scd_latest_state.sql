{%- macro scd_latest_state(source='base', max_column='_task_instance', upload_column='_uploaded_at', primary_key='id') -%}

, max_task_instance AS (
    SELECT MAX({{ max_column }}) AS max_column_value
    FROM {{ source }}
    WHERE RIGHT( {{ max_column }}, 8) = (

                                SELECT MAX(RIGHT( {{ max_column }}, 8))
                                FROM {{ source }} )

), filtered AS (

    SELECT *
    FROM {{ source }}
    WHERE {{ max_column }} = (

                            SELECT max_column_value
                            FROM max_task_instance

                            )
    -- Keep only the latest state of the data,
    -- if we have multiple records per day
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ upload_column }} DESC) = 1
)

SELECT *
FROM filtered

{%- endmacro -%}
