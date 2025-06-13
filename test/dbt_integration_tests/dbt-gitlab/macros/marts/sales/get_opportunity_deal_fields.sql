{% macro get_opportunity_deal_fields() %}
    {% set deal_fields = [
        'deal_path',
        'opportunity_deal_size',
        'deal_category',
        'deal_group',
        'deal_size',
        'calculated_deal_size',
        'deal_path_engagement'
    ] %}
    {{ return(deal_fields) }}
{% endmacro %}