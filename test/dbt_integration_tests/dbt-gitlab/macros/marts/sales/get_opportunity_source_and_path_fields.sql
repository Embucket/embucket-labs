{% macro get_opportunity_source_and_path_fields() %}
    {% set source_path_fields = [
        'primary_campaign_source_id',
        'generated_source',
        'lead_source',
        'net_new_source_categories',
        'sales_qualified_source',
        'sales_qualified_source_grouped',
        'sales_path',
        'subscription_type',
        'source_buckets',
        'opportunity_development_representative',
        'sdr_or_bdr',
        'iqm_submitted_by_role',
        'sdr_pipeline_contribution'
    ] %}
    {{ return(source_path_fields) }}
{% endmacro %}