{%- macro visibility_level_name(visibility_level_id) -%}

    CASE
      WHEN {{visibility_level_id}}::NUMBER = '20' THEN 'public'
      WHEN {{visibility_level_id}}::NUMBER = '10' THEN 'internal'
      ELSE 'private'
    END::VARCHAR

{%- endmacro -%}
CASE
