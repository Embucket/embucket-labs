{% macro get_opportunity_command_plan_fields() %}
    {% set command_plan_fields = [
        'cp_partner',
        'cp_paper_process',
        'cp_help',
        'cp_review_notes',
        'cp_use_cases',
        'cp_champion',
        'cp_close_plan',
        'cp_decision_criteria',
        'cp_decision_process',
        'cp_economic_buyer',
        'cp_identify_pain',
        'cp_metrics',
        'cp_risks',
        'cp_value_driver',
        'cp_why_do_anything_at_all',
        'cp_why_gitlab',
        'cp_why_now',
        'cp_score'
    ] %}
    {{ return(command_plan_fields) }}
{% endmacro %}