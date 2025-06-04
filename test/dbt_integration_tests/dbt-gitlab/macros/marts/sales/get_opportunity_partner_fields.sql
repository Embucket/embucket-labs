{% macro get_opportunity_partner_fields() %}
    {% set partner_fields = [
        'dr_partner_deal_type',
        'dr_partner_engagement',
        'dr_primary_registration',
        'dr_status',
        'dr_deal_id',
        'aggregate_partner',
        'partner_initiated_opportunity',
        'calculated_partner_track',
        'partner_account',
        'partner_discount',
        'partner_discount_calc',
        'partner_margin_percentage',
        'partner_track',
        'platform_partner',
        'resale_partner_track',
        'resale_partner_name',
        'fulfillment_partner',
        'fulfillment_partner_account_name',
        'fulfillment_partner_partner_track',
        'partner_account_account_name',
        'partner_account_partner_track',
        'influence_partner',
        'comp_channel_neutral',
        'distributor'
    ] %}
    {{ return(partner_fields) }}
{% endmacro %}