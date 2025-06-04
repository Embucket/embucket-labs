{% macro hash_diff(cte_ref, return_cte, columns) %}

  , hashing AS (

    SELECT *,

      {{ dbt_utils.generate_surrogate_key( columns ) }} as prev_hash

    FROM {{ cte_ref }}

  ), {{ return_cte }} as (

    {%- set columns = adapter.get_columns_in_relation(this) -%}

    {%- set column_names = [] -%}

    {%- for column in columns -%}

      {%- set _ = column_names.append(column.name) -%}

    {% endfor %}

      {% if 'LAST_CHANGED' in column_names %}

        SELECT hashing.*,
          CASE
            WHEN hashing.prev_hash = t.prev_hash THEN last_changed
            ELSE CURRENT_TIMESTAMP()
          END AS last_changed
        FROM hashing
        LEFT JOIN {{ this }} as t on t.prev_hash = hashing.prev_hash

      {% else %}

        SELECT *,
        CURRENT_TIMESTAMP() AS last_changed
        FROM hashing

      {% endif %}

  )
{% endmacro %}