{{ config(alias='report_agg_demo_sqs_ot_keys') }}
-- supports FY22, FY23 grains

WITH report_agg_keys_base AS (

    SELECT *
    FROM {{ref('wk_sales_report_agg_keys_base')}}

), eligible AS (

    SELECT DISTINCT
        report_opportunity_user_segment,
        report_opportunity_user_geo,
        report_opportunity_user_region,
        report_opportunity_user_area,

        sales_qualified_source,
        deal_group,

        report_user_segment_geo_region_area,
        report_user_segment_geo_region_area_sqs_ot,

        key_segment,
        key_sqs,
        key_ot,

        key_segment_sqs,
        key_segment_ot,    

        key_segment_geo,
        key_segment_geo_sqs,
        key_segment_geo_ot,      

        key_segment_geo_region,
        key_segment_geo_region_sqs,
        key_segment_geo_region_ot,   

        key_segment_geo_region_area,
        key_segment_geo_region_area_sqs,
        key_segment_geo_region_area_ot,

        key_segment_geo_area,
        sales_team_cro_level,
        sales_team_rd_asm_level,
        sales_team_vp_level,
        sales_team_avp_rd_level,
        sales_team_asm_level

  FROM report_agg_keys_base

)

SELECT *
FROM eligible