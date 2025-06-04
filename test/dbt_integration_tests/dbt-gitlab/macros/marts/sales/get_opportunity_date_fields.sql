{% macro get_opportunity_date_fields(date_mappings) %}
    
      {%- set date_mappings = [
        'created_date_id',
        'sales_accepted_date_id',
        'sales_qualified_date_id',
        'close_date_id',
        ('arr_created_date_id', 'arr_created'),
        ('arr_created_date_id', 'pipeline_created'),
        ('arr_created_date_id', 'net_arr_created'),
        'last_activity_date_id',
        'sales_last_activity_date_id',
        'subscription_start_date_id',
        'subscription_end_date_id',
        'subscription_renewal_date_id',
        'technical_evaluation_date_id',
        'stage_0_pending_acceptance_date_id',
        'stage_1_discovery_date_id',
        'stage_2_scoping_date_id',
        'stage_3_technical_evaluation_date_id',
        'stage_4_proposal_date_id',
        'stage_5_negotiating_date_id',
        ('stage_6_awaiting_signature_date_id','stage_6_awaiting_signature_date'),
        'stage_6_closed_won_date_id',
        'stage_6_closed_lost_date_id'
    ] -%}
    
    SELECT
        -- Field selections
        dim_crm_opportunity_id,
        {% for mapping in date_mappings %}
            {% if mapping is string %}
                {# Handle simple string case - use default naming #}
                {% set date_column = mapping %}
                {% set date_alias = date_column | replace('_id', '') %}
                {% set field_prefix = date_alias | replace('_date', '') %}
                {% set join_alias = date_alias %}
            {% else %}
                {# Handle tuple case with custom alias #}
                {% set date_column = mapping[0] %}
                {% set date_alias = mapping[1] + '_date' %}  {# Create unique join alias using the custom prefix #}
                {% set field_prefix = mapping[1] %}
                {% set join_alias = date_alias %}
            {% endif %}
            
        {{ join_alias }}.date_actual                               AS {{ field_prefix }}_date,
        {{ join_alias }}.first_day_of_month                        AS {{ field_prefix }}_month,
        {{ join_alias }}.first_day_of_fiscal_quarter               AS {{ field_prefix }}_fiscal_quarter_date,
        {{ join_alias }}.fiscal_quarter_name_fy                    AS {{ field_prefix }}_fiscal_quarter_name,
        {{ join_alias }}.fiscal_year                               AS {{ field_prefix }}_fiscal_year
            {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ ref('fct_crm_opportunity', v=2) }}
        {% for mapping in date_mappings %}
            {% if mapping is string %}
                {% set date_column = mapping %}
                {% set join_alias = date_column | replace('_id', '') %}
            {% else %}
                {% set date_column = mapping[0] %}
                {% set join_alias = mapping[1] + '_date' %}  {# Use same unique join alias as in SELECT #}
            {% endif %}
        LEFT JOIN {{ ref('dim_date') }} AS {{ join_alias }}
            ON {{ ref('fct_crm_opportunity', v=2) }}.{{ date_column }} = {{ join_alias }}.date_id
        {% endfor %}

{%- endmacro %}