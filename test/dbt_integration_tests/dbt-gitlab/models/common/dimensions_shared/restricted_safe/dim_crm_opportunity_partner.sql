{{ config(
    tags=["six_hourly"]
) }}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE
    
), distinct_values AS (

    SELECT DISTINCT
        dr_partner_deal_type,
        dr_partner_engagement,         
        dr_status,
        dr_primary_registration,
        dr_deal_id,
        aggregate_partner,
        partner_initiated_opportunity,
        calculated_partner_track,
        partner_account,
        partner_discount,
        partner_discount_calc,
        partner_margin_percentage,
        partner_track,
        platform_partner,
        resale_partner_track,
        resale_partner_name,
        fulfillment_partner,
        fulfillment_partner_account_name, 
        fulfillment_partner_partner_track,
        partner_account_account_name,
        partner_account_partner_track,
        influence_partner,
        comp_channel_neutral,
        distributor

    FROM prep_crm_opportunity

), unioned AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(get_opportunity_partner_fields()) }} AS dim_crm_opportunity_partner_sk,                                                                                                                                                                                                                                         
        *
    FROM distinct_values

    UNION ALL

    -- Missing member info
    SELECT
      MD5('-1')                                                                  AS dim_crm_opportunity_partner_sk,
      'Missing dr_partner_deal_type'                                             AS dr_partner_deal_type,
      'Missing dr_partner_engagement'                                            AS dr_partner_engagement,
      'Missing dr_primary_registration'                                          AS dr_primary_registration,
      'Missing dr_status'                                                        AS dr_status,
      'Missing dr_deal_id'                                                       AS dr_deal_id,    
      'Missing aggregate_partner'                                                AS aggregate_partner,
      FALSE                                                                      AS partner_initiated_opportunity,
      'Missing calculated_partner_track'                                         AS calculated_partner_track,
      'Missing partner_account'                                                  AS partner_account,
      0                                                                          AS partner_discount,
      0                                                                          AS partner_discount_calc,
      0                                                                          AS partner_margin_percentage,
      'Missing partner_track'                                                    AS partner_track,
      'Missing platform_partner'                                                 AS platform_partner,
      'Missing resale_partner_track'                                             AS resale_partner_track,
      'Missing resale_partner_name'                                              AS resale_partner_name,
      'Missing fulfillment_partner'                                              AS fulfillment_partner,
      'Missing fulfillment_partner_account_name'                                 AS fulfillment_partner_account_name,
      'Missing fulfillment_partner_partner_track'                                AS fulfillment_partner_partner_track,
      'Missing partner_account_account_name'                                     AS partner_account_account_name,
      'Missing partner_account_partner_track'                                    AS partner_account_partner_track,
      'Missing influence partner'                                                AS influence_partner,
      0                                                                          AS comp_channel_neutral,
      'Missing distributor'                                                      AS distributor
)

SELECT *
FROM unioned