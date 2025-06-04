{{ config(alias='sfdc_users_xf') }}

WITH source_user AS (

    SELECT 
        sfdc_users.id                   AS user_id,
        sfdc_users.user_segment__c      AS user_segment,
        sfdc_users.user_role_type__c    AS user_role_type,
        sfdc_user_roles_source.name     AS user_role_name,
        COALESCE(CAST(REPLACE(
                REPLACE(sfdc_users.hybrid__c,'Yes','1')
                ,'No','0') 
            AS INTEGER),0)              AS is_hybrid_flag
    FROM {{ source('salesforce', 'user') }} sfdc_users
    LEFT JOIN {{ ref('sfdc_user_roles_source') }} sfdc_user_roles_source
      ON sfdc_users.userroleid = sfdc_user_roles_source.id

),  base AS (
    SELECT
      edm_user.dim_crm_user_id           AS user_id,
      edm_user.user_name                 AS name,
      edm_user.department,
      edm_user.title,
      edm_user.team,
      CASE --only expose GitLab.com email addresses of internal employees
        WHEN edm_user.user_email LIKE '%gitlab.com' THEN edm_user.user_email ELSE NULL
      END                       AS user_email,
      edm_user.manager_name,
      edm_user.manager_id,
      
      IFNULL(edm_user.crm_user_geo, 'Other')                AS user_geo,
      IFNULL(edm_user.crm_user_region, 'Other')             AS user_region,


      edm_user.crm_user_business_unit,

      CASE 
        WHEN LOWER(source_user.user_segment) = 'lrg' 
          THEN 'Large'
        WHEN LOWER(source_user.user_segment) = 'mm' 
          THEN 'Mid-Market' 
        WHEN LOWER(source_user.user_segment) = 'jihu' 
          THEN 'Jihu'         
        WHEN LOWER(source_user.user_segment) = 'all' 
          THEN 'All'        
        ELSE
          IFNULL(source_user.user_segment, 'Other') 
      END                                                   AS final_user_segment,

      LOWER(source_user.user_segment)                       AS raw_user_segment,

        -- JK 2023-02-06 adding adjusted segment
        -- If MM / SMB and Region = META then Segment = Large
        -- If MM/SMB and Region = LATAM then Segment = Large
        -- If MM/SMB and Geo = APAC then Segment = Large
        -- Use that Adjusted Segment Field in our FY23 models
        CASE
        WHEN LOWER(crm_user_business_unit) = 'japan'
            THEN 'Japan'
        WHEN (LOWER(final_user_segment) = 'mid-market'
                OR LOWER(final_user_segment)  = 'smb')
            AND LOWER(user_region) = 'meta'
            THEN 'Large'
        WHEN (LOWER(final_user_segment)  = 'mid-market'
                OR LOWER(final_user_segment)  = 'smb')
            AND LOWER(user_region) = 'latam'
            THEN 'Large'
        WHEN (LOWER(final_user_segment)  = 'mid-market'
                OR LOWER(final_user_segment)  = 'smb')
            AND LOWER(user_geo) = 'apac'
            THEN 'Large'
        WHEN LOWER(source_user.user_segment) = 'all' 
          THEN 'Large'     
        ELSE final_user_segment
        END                                            AS adjusted_user_segment,

      IFNULL(edm_user.crm_user_area, 'Other')          AS user_area,
      IFNULL(edm_user.user_role_name, 'Other')         AS role_name,
      IFNULL(edm_user.user_role_type, 'Other')         AS role_type,
      edm_user.start_date,
      edm_user.is_active,
      edm_user.employee_number,

      source_user.is_hybrid_flag

    FROM {{ref('dim_crm_user')}} edm_user
    LEFT JOIN source_user
        ON edm_user.dim_crm_user_id = source_user.user_id

), consolidation AS (
    SELECT
      base.user_id,
      base.name,
      base.department,
      base.title,
      base.team,
      base.user_email,
      base.manager_name,
      base.manager_id,
      base.user_geo,
      base.user_region,
      -- NF: adjusted to account for the updates the data team ran on source
      -- Needed to adjust ALL to Large
      base.final_user_segment AS user_segment,
      base.raw_user_segment,
      base.adjusted_user_segment,
      base.user_area,
      base.role_name,
      base.role_type,
      base.start_date,
      base.is_active,
      base.is_hybrid_flag,
      base.employee_number,
      base.crm_user_business_unit,

     
      CASE
        WHEN LOWER(title) LIKE '%strategic account%'
           OR LOWER(title) LIKE '%account executive%'
           OR LOWER(title) LIKE '%country manager%'
           OR LOWER(title) LIKE '%public sector channel manager%'
           OR LOWER(role_name) LIKE '%ae_%'
        THEN 1
        ELSE 0
      END                                                                                          AS is_rep_flag

    FROM base

), user_based_reporting_keys AS (
    SELECT
      consolidation.*,

      -- Business Unit (X-Ray 1st hierarchy)
      -- will be replaced with the actual field
      /*CASE 
        WHEN LOWER(user_segment) IN ('large','pubsec','all') -- "all" segment is PubSec for ROW
            THEN 'ENTG'
        WHEN LOWER(user_region) IN ('latam','meta')
            OR LOWER(user_geo) IN ('apac')
            THEN 'ENTG'         
        WHEN LOWER(user_segment) IN ('mid-market','smb')
            THEN 'COMM'
        WHEN LOWER(user_segment) = 'jihu' THEN 'JiHu'
        ELSE 'Other'
      END
      */ 
      
      crm_user_business_unit AS business_unit,

      -- Sub-Business Unit (X-Ray 2nd hierarchy)
      /*
      JK 2023-02-06: sub-BU is used in lower hierarchy fields calculation (division & asm).
      Therefore when making changes to the field, make sure to understand implications on the whole key hierarchy
      */
      CASE
        WHEN LOWER(business_unit) = 'entg'
          THEN user_geo
        -- H2 update
        WHEN LOWER(business_unit) = 'japan'
          THEN 'Japan'      
        WHEN
          LOWER(business_unit) = 'comm'
          AND
            (
            LOWER(user_segment) = 'mid-market'
            AND LOWER(role_type) = 'fo'
            )
          THEN 'MM First Orders'  --mid-market FO(?)
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(user_geo) = 'emea'
          THEN  'EMEA'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(user_geo) = 'amer'
          THEN 'AMER'
        ELSE 'Other'
      END AS sub_business_unit,

      -- Division (X-Ray 3rd hierarchy)
      CASE 
        WHEN LOWER(business_unit) = 'entg'
          THEN user_region
        WHEN LOWER(business_unit) = 'japan'
          THEN 'Japan'   
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(user_segment) = 'mid-market'         
          AND LOWER(sub_business_unit) = 'mm first orders'
          THEN 'MM First Orders'
        WHEN 
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) != 'mm first orders'
          AND LOWER(user_segment) = 'mid-market'
          THEN 'Mid-Market'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) != 'mm first orders'
          AND LOWER(user_segment) = 'smb'
          THEN 'SMB'
        ELSE 'Other'
      END AS division,

      -- ASM (X-Ray 4th hierarchy): definition pending
      CASE
        WHEN LOWER(business_unit) = 'japan'
          THEN user_area 
        WHEN 
          LOWER(business_unit) = 'entg'
          THEN user_area  
        WHEN 
          LOWER(business_unit) = 'comm'
          AND LOWER(division) = 'mm first orders'
          THEN user_geo
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(division) IN ('smb','mid-market')
          THEN user_area
        ELSE 'Other'
      END AS asm
    FROM consolidation

), final AS (

    SELECT *
    FROM user_based_reporting_keys

)

SELECT *,

    LOWER(business_unit)                                                              AS key_bu,
    LOWER(business_unit || '_' || sub_business_unit)                                  AS key_bu_subbu,
    LOWER(business_unit || '_' || sub_business_unit || '_' || division)               AS key_bu_subbu_division,
    LOWER(business_unit || '_' || sub_business_unit || '_' || division || '_' || asm) AS key_bu_subbu_division_asm,
    LOWER(key_bu_subbu_division_asm || '_' || role_type || '_' || TO_VARCHAR(employee_number))  AS key_sal_heatmap

FROM final