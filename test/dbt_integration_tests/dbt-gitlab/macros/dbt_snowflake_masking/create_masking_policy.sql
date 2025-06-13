{%- macro create_masking_policy(database, schema, data_type, policy) -%}

{%- set mask = get_mask(data_type) -%}
{% set body %}
  CASE 
    WHEN CURRENT_ROLE() IN ('TRANSFORMER','LOADER') THEN val  -- Set for specific roles that should always have access
    WHEN IS_ROLE_IN_SESSION('{{ policy }}') THEN val -- Set for the user to inherit access bases on there roles
    ELSE {{ mask }} 
  END 
{% endset %}

{% set policy_name %}
"{{ database }}".{{ schema }}.{{ policy }}_{{ data_type }}
{% endset %}

CREATE MASKING POLICY IF NOT EXISTS {{ policy_name }} AS (val {{ data_type }}) 
  RETURNS {{ data_type }} ->
  {{ body }};

ALTER MASKING POLICY IF EXISTS {{ policy_name }} SET BODY -> 
  {{ body }};

{%- endmacro -%}
