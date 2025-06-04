{% macro ref() %}
  -- extract user-provided positional and keyword arguments
  {% set version = kwargs.get('version') or kwargs.get('v') %}
  {% set package_name = none %}
  {%- if (varargs | length) == 1 -%}
      {% set model_name = varargs[0] %}
  {%- else -%}
      {% set package_name = varargs[0] %}
      {% set model_name = varargs[1] %}
  {% endif %}

  -- call builtins.ref without a version if none is provided
  {% set relation = None %}

  {% if version is not none %}
      {% if package_name is not none %}
          {% set relation = builtins.ref(package_name, model_name, version=version) %}
      {% else %}
          {% set relation = builtins.ref(model_name, version=version) %}
      {% endif %}
  {% else %}
      {% if package_name is not none %}
          {% set relation = builtins.ref(package_name, model_name) %}
      {% else %}
          {% set relation = builtins.ref(model_name) %}
      {% endif %}
  {% endif %}

  -- enable sampling
  {% if is_table_sampled(model_name) %}
    {% set new_relation = get_sample_relation(relation) %}
  {% else %}
    {% set new_relation = relation %}
  {% endif %}
  
  {% do return(new_relation) %}
{% endmacro %}
