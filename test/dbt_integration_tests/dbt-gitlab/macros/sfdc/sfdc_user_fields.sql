{%- macro sfdc_user_fields(model_type) %}

{{ simple_cte([
    ('sfdc_user_roles_source','sfdc_user_roles_source'),
    ('dim_date','dim_date'),
    ('sfdc_users_source', 'sfdc_users_source'),
    ('sfdc_user_snapshots_source', 'sfdc_user_snapshots_source')
]) }}

, sheetload_mapping_sdr_sfdc_bamboohr_source AS (

    SELECT *
    FROM {{ ref('sheetload_mapping_sdr_sfdc_bamboohr_source') }}

{%- if model_type == 'snapshot' %}

), snapshot_dates AS (

    SELECT *
    FROM dim_date
    WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT MAX(snapshot_id) FROM {{ this }})

   {% endif %}
{%- endif %}

), sfdc_users AS (

    SELECT 
      {%- if model_type == 'live' %}
        *
      {%- elif model_type == 'snapshot' %}
      {{ dbt_utils.generate_surrogate_key(['sfdc_user_snapshots_source.user_id','snapshot_dates.date_id'])}}    AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                           AS snapshot_id,
      snapshot_dates.fiscal_year                                                                       AS snapshot_fiscal_year,
      snapshot_dates.date_actual                                                                       AS snapshot_date,
      sfdc_user_snapshots_source.*
      {%- endif %}
    FROM
      {%- if model_type == 'live' %}
      sfdc_users_source
      {%- elif model_type == 'snapshot' %}
      sfdc_user_snapshots_source
      INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= sfdc_user_snapshots_source.dbt_valid_from
        AND snapshot_dates.date_actual < COALESCE(sfdc_user_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}

), current_fiscal_year AS (

    SELECT 
      fiscal_year
    FROM dim_date
    WHERE date_actual = CURRENT_DATE()

), final AS (

    SELECT
      {%- if model_type == 'snapshot' %}
      sfdc_users.crm_user_snapshot_id,
      sfdc_users.snapshot_id,
      sfdc_users.snapshot_date,
      {%- endif %}
      sfdc_users.user_id                                                                                                              AS dim_crm_user_id,
      sfdc_users.employee_number,
      sfdc_users.name                                                                                                                 AS user_name,
      sfdc_users.title,
      sfdc_users.department,
      sfdc_users.team,
      sfdc_users.manager_id,
      sfdc_users.manager_name,
      sfdc_users.user_email,
      sfdc_users.is_active,
      sfdc_users.start_date,
      sfdc_users.ramping_quota,
      sfdc_users.user_timezone,
      sfdc_users.user_role_id,
      {{ dbt_utils.generate_surrogate_key(['sfdc_user_roles_source.name']) }}                                                         AS dim_crm_user_role_name_id,
      sfdc_user_roles_source.name                                                                                                     AS user_role_name,
      sfdc_users.user_role_type                                                                                                       AS user_role_type,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_level_1']) }}                                                        AS dim_crm_user_role_level_1_id,
      sfdc_users.user_role_level_1                                                                                                    AS user_role_level_1,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_level_2']) }}                                                        AS dim_crm_user_role_level_2_id,
      sfdc_users.user_role_level_2                                                                                                    AS user_role_level_2,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_level_3']) }}                                                        AS dim_crm_user_role_level_3_id,
      sfdc_users.user_role_level_3                                                                                                    AS user_role_level_3,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_level_4']) }}                                                        AS dim_crm_user_role_level_4_id,
      sfdc_users.user_role_level_4                                                                                                    AS user_role_level_4,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_level_5']) }}                                                        AS dim_crm_user_role_level_5_id,
      sfdc_users.user_role_level_5                                                                                                    AS user_role_level_5,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_segment']) }}                                                             AS dim_crm_user_sales_segment_id,
      sfdc_users.user_segment                                                                                                         AS crm_user_sales_segment,
      sfdc_users.user_segment_grouped                                                                                                 AS crm_user_sales_segment_grouped,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_geo']) }}                                                                 AS dim_crm_user_geo_id,
      sfdc_users.user_geo                                                                                                             AS crm_user_geo,
      IFF(LOWER(sfdc_users.user_segment) = LOWER('pubsec'), CONCAT(sfdc_users.user_geo, '-', UPPER(sfdc_users.user_segment)), sfdc_users.user_geo) AS crm_user_geo_pubsec_segment,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_region']) }}                                                              AS dim_crm_user_region_id,
      sfdc_users.user_region                                                                                                          AS crm_user_region,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_area']) }}                                                                AS dim_crm_user_area_id,
      sfdc_users.user_area                                                                                                            AS crm_user_area,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_business_unit']) }}                                                       AS dim_crm_user_business_unit_id,
      sfdc_users.user_business_unit                                                                                                   AS crm_user_business_unit,
      {{ dbt_utils.generate_surrogate_key(['sfdc_users.user_role_type']) }}                                                           AS dim_crm_user_role_type_id,
      CASE 
        WHEN sfdc_users.is_hybrid_user = 'Yes' 
          THEN 1
        WHEN sfdc_users.is_hybrid_user = 'No' 
          THEN  0
        WHEN sfdc_users.is_hybrid_user IS NULL 
          THEN 0
        ELSE 0 
      END                                                                                                                             AS is_hybrid_user,
      {%- if model_type == 'live' %}
      CONCAT(
             UPPER(sfdc_user_roles_source.name),
             '-',
             current_fiscal_year.fiscal_year
            )                                                                                                                         AS dim_crm_user_hierarchy_sk,
      {%- elif model_type == 'snapshot' %}
      CASE
        WHEN sfdc_users.snapshot_fiscal_year < 2024
          THEN CONCAT(
                      UPPER(sfdc_users.user_segment), 
                      '-',
                      UPPER(sfdc_users.user_geo), 
                      '-',
                      UPPER(sfdc_users.user_region), 
                      '-',
                      UPPER(sfdc_users.user_area),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )
        WHEN sfdc_users.snapshot_fiscal_year = 2024 AND LOWER(sfdc_users.user_business_unit) = 'comm'
          THEN CONCAT(
                      UPPER(sfdc_users.user_business_unit), 
                      '-',
                      UPPER(sfdc_users.user_geo), 
                      '-',
                      UPPER(sfdc_users.user_segment), 
                      '-',
                      UPPER(sfdc_users.user_region), 
                      '-',
                      UPPER(sfdc_users.user_area),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )
        WHEN sfdc_users.snapshot_fiscal_year = 2024 AND LOWER(sfdc_users.user_business_unit) = 'entg'
          THEN CONCAT(
                      UPPER(sfdc_users.user_business_unit), 
                      '-',
                      UPPER(sfdc_users.user_geo), 
                      '-',
                      UPPER(sfdc_users.user_region), 
                      '-',
                      UPPER(sfdc_users.user_area), 
                      '-',
                      UPPER(sfdc_users.user_segment),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )
        WHEN sfdc_users.snapshot_fiscal_year = 2024
          AND (sfdc_users.user_business_unit IS NOT NULL AND LOWER(sfdc_users.user_business_unit) NOT IN ('comm', 'entg'))  -- account for non-sales reps
          THEN CONCAT(
                      UPPER(sfdc_users.user_business_unit), 
                      '-',
                      UPPER(sfdc_users.user_segment), 
                      '-',
                      UPPER(sfdc_users.user_geo), 
                      '-',
                      UPPER(sfdc_users.user_region), 
                      '-',
                      UPPER(sfdc_users.user_area),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )

        WHEN sfdc_users.snapshot_fiscal_year = 2024 AND sfdc_users.user_business_unit IS NULL -- account for nulls/possible data issues
          THEN CONCAT(
                      UPPER(sfdc_users.user_segment), 
                      '-',
                      UPPER(sfdc_users.user_geo), 
                      '-',
                      UPPER(sfdc_users.user_region), 
                      '-',
                      UPPER(sfdc_users.user_area),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )
        WHEN sfdc_users.snapshot_fiscal_year >= 2025
          THEN CONCAT(
                      UPPER(sfdc_user_roles_source.name),
                      '-',
                      sfdc_users.snapshot_fiscal_year
                      )        
        END                                                                                                                           AS dim_crm_user_hierarchy_sk,
      {%- endif %}
      COALESCE(
               sfdc_users.user_segment_geo_region_area,
               CONCAT(sfdc_users.user_segment,'-' , sfdc_users.user_geo, '-', sfdc_users.user_region, '-', sfdc_users.user_area)
               )                                                                                                                      AS crm_user_sales_segment_geo_region_area,
      sfdc_users.user_segment_region_grouped                                                                                          AS crm_user_sales_segment_region_grouped,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_segment                                                                          AS sdr_sales_segment,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region,
      sfdc_users.created_date,
         CASE
        WHEN LOWER(sfdc_users.user_business_unit) = 'entg'
          THEN user_geo

        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
          AND
            (
            LOWER(sfdc_users.user_segment) = 'smb'
            AND LOWER(sfdc_users.user_geo) = 'amer'
            AND LOWER(sfdc_users.user_area) = 'lowtouch'
            ) 
          THEN 'AMER Low-Touch'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
          AND
            (
            LOWER(sfdc_users.user_segment) = 'mid-market'
            AND (LOWER(sfdc_users.user_geo) = 'amer' OR LOWER(sfdc_users.user_geo) = 'emea')
            AND LOWER(sfdc_users.user_role_type) = 'first order'
            )
          THEN 'MM First Orders'  --mid-market FO(?)
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
          AND LOWER(sfdc_users.user_geo) = 'emea'
          AND 
            (
            LOWER(sfdc_users.user_segment) != 'mid-market'
            AND LOWER(sfdc_users.user_role_type) != 'first order'
            )
          THEN  'EMEA'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
          AND LOWER(sfdc_users.user_geo) = 'amer'
          AND
            (
            LOWER(sfdc_users.user_segment) != 'mid-market'
            AND LOWER(sfdc_users.user_role_type) != 'first order'
            )
          AND
            (
            LOWER(sfdc_users.user_segment) != 'smb'
            AND LOWER(sfdc_users.user_area) != 'lowtouch'
            )
          THEN 'AMER'
        ELSE 'Other'
      END                                                                                                                             AS crm_user_sub_business_unit,

      -- Division (X-Ray 3rd hierarchy)
      CASE 
        WHEN LOWER(sfdc_users.user_business_unit) = 'entg'
          THEN sfdc_users.user_region

        WHEN 
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND (LOWER(crm_user_sub_business_unit) = 'amer' OR LOWER(crm_user_sub_business_unit) = 'emea')
              AND LOWER(sfdc_users.user_segment) = 'mid-market'
          THEN 'Mid-Market'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND (LOWER(crm_user_sub_business_unit) = 'amer' OR LOWER(crm_user_sub_business_unit) = 'emea')
              AND LOWER(sfdc_users.user_segment) = 'smb'
          THEN 'SMB'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND LOWER(crm_user_sub_business_unit) = 'mm first orders'
          THEN 'MM First Orders'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
          THEN 'AMER Low-Touch'
        ELSE 'Other'
      END                                                                                                                             AS crm_user_division,

      -- ASM (X-Ray 4th hierarchy): definition pending
      CASE
        WHEN 
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'amer'
          THEN sfdc_users.user_area
        WHEN 
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'emea'
              AND (LOWER(crm_user_division) = 'dach' OR LOWER(crm_user_division) = 'neur' OR LOWER(crm_user_division) = 'seur')
          THEN sfdc_users.user_area
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'emea'
              AND LOWER(crm_user_division) = 'meta'
          THEN sfdc_users.user_segment --- pending/ waiting for Meri?
        WHEN 
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'apac'
          THEN sfdc_users.user_area
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'pubsec'
              AND LOWER(crm_user_division) != 'sled'
          THEN sfdc_users.user_area
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'entg'
            AND LOWER(crm_user_sub_business_unit) = 'pubsec'
              AND LOWER(crm_user_division) = 'sled'
          THEN sfdc_users.user_region

        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND (LOWER(crm_user_sub_business_unit) = 'amer' OR LOWER(crm_user_sub_business_unit) = 'emea')
          THEN sfdc_users.user_area
        WHEN 
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND LOWER(crm_user_sub_business_unit) = 'mm first orders'
          THEN sfdc_users.user_geo
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
              AND LOWER(sfdc_users.user_role_type) = 'first order'
          THEN 'LowTouch FO'
        WHEN
          LOWER(sfdc_users.user_business_unit) = 'comm'
            AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
              AND LOWER(sfdc_users.user_role_type) != 'first order'
          THEN 'LowTouch Pool'
        ELSE 'Other'
      END                                                                                                         AS asm
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles_source
      ON sfdc_users.user_role_id = sfdc_user_roles_source.id
    LEFT JOIN sheetload_mapping_sdr_sfdc_bamboohr_source
      ON sfdc_users.user_id = sheetload_mapping_sdr_sfdc_bamboohr_source.user_id
    LEFT JOIN current_fiscal_year

)

{%- endmacro %}