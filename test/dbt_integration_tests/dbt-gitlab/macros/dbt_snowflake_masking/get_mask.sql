{%- macro get_mask(data_type, policy=none) -%}

  {# Default maksing for each data type #}
  {% if data_type == 'TEXT' %}
    {% set mask = '\'***MASKED***\''%}
  {% elif data_type == 'TIMESTAMP_NTZ' %}
    {% set mask = 'NULL'%}
  {% elif data_type == 'ARRAY' %}
    {% set mask = '[\'***MASKED***\']'%}
  {% elif data_type == 'VARIANT' %}
    {% set mask = '\'{***MASKED***}\''%}
  {% elif data_type == 'DATE' %}
    {% set mask = 'NULL'%}
  {% elif data_type == 'FLOAT' %}
    {% set mask = '0.0'%}
  {% elif data_type == 'NUMBER' %}
    {% set mask = '0'%}
  {% elif data_type == 'BOOLEAN' %}
    {% set mask = 'NULL'%}
  {% else %}
    {% set mask = 'NULL'%}
  {% endif %}

  {% if policy %}
    {# Override default masking on a policy specific basis #}
  {% endif %}


  {{ return(mask) }}

{% endmacro %}