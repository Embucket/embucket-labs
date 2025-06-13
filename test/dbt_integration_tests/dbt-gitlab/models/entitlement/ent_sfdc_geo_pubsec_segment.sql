{{ config(
  materialized='table'
) }}

WITH target_geos AS (
  -- This table is the target subset of the assigned user geo values in Salesforce
  SELECT *
  FROM (
    VALUES
    ('AMER'),
    ('EMEA'),
    ('APJ'),
    ('APAC'),
    ('JAPAN'),
    ('PUBSEC'),
    ('SMB'),
    ('CHANNEL')
  ) AS geos (target_geos)
),

target_user_roles AS (
  -- This table is a list of the user roles and PUBSEC access of those roles
  -- for those Salesforce users that do not have an assigned geo
  SELECT *
  FROM (
    VALUES
    ('CMO', TRUE),
    ('CRO', TRUE),
    ('Director - SA High Velocity', TRUE),
    ('VP_EMEA', TRUE),
    ('VP_AMER', TRUE),
    ('XDR Operations Sr.Manager_ALL_ALL_ALL', TRUE),
    ('Director CSM', TRUE),
    ('Director of Professional Services', TRUE),
    ('Executive', TRUE),
    ('Field CTO', TRUE),
    ('Field Marketing Director', TRUE),
    ('Field Marketing Manager_Global', TRUE),
    ('Global CSE Director', TRUE),
    ('VP Sales Development', TRUE),
    ('VP PARTNERS', TRUE),
    ('VPSA', TRUE),
    ('RM_ALL', TRUE),
    ('Director XDR Operations ALL_ ALL_ ALL', TRUE),
    ('XDR Operations Senior Associate ALL_ ALL_ ALL', TRUE),
    ('XDR Operations Sr.Manager', TRUE),
    ('Global XDREnablement Manager ALL ALL ALL', TRUE),
    ('Global XDREnablement Manager', TRUE),
    ('Global SDR Manager', FALSE),
    ('RSD ENTG', TRUE),
    ('Executive - Global Minus Pubsec', FALSE),
    ('Implementation Engineers - Global (with/without Pubsec)', FALSE),
    ('Marketing Operations Manager', FALSE),
    ('Marketing Program Manager', FALSE),
    ('ABM Manager', FALSE),
    ('VP_APJ', TRUE),
    ('VP_SMB', TRUE),
    ('Director - COE', TRUE),
    ('Manager - SA - Partner', TRUE),
    ('Manager - BVS', TRUE),
    ('VP_Sales_Development_ALL_ALL', TRUE),
    ('Director XDR Operations', TRUE),
    ('XDR Operations Manager', TRUE),
    ('CSEM_ALL', TRUE),
    ('CSDIR_AMER', TRUE),
    ('CSAM_ALL', TRUE),
    ('CSDIR_APJ', TRUE),
    ('CSDIR_AMER_PUBSEC', TRUE),
    ('CSDIR_ALL', TRUE),
    ('CSVP_ALL', TRUE),
    ('Engagement Manager_All_PubSec_Inclusive', TRUE),
    ('VP_Professional_Services', TRUE),
    ('Director_of_Professional_Services', TRUE),
    ('ESM_GSI', TRUE),
    ('CL_PROGRAM_ED', TRUE),
    ('ESM_GLOBAL', TRUE),
    ('CH_PGM_NONUS', TRUE),
    ('CH_PGM_US', TRUE),
    ('CL_PGM_US', TRUE),
    ('CH_PROGRAM_ED', TRUE),
    ('ED_AWS', TRUE),
    ('ED_GCP', TRUE),
    ('ED_GSI', TRUE),
    ('VP_ECOSYSTEM', TRUE)
  ) AS roles (target_roles, has_pubsec)
),

override_crm_users AS (
  -- This table is the list of crm users where it is needed to override the geo assignment and grant access to everything
  SELECT *
  FROM (
    VALUES
      ('0054M000004PAFYQA4')
    ) AS geos (crm_user_id)
),

override_pubsec_user_roles AS (
  -- This table is the list of crm user roles where it is needed to override the geo assignment and grant access to PubSec accounts
  SELECT *
  FROM (
    VALUES
      ('00E8X000001vcEQUAY'),
      ('00E8X000001vcEfUAI'),
      ('00E8X000001vcDXUAY')
    ) AS geos (crm_user_role_id)
),

smb_user_roles AS (
-- This table is the list of user roles who have access to SMB opps and accounts
SELECT *
  FROM (
    VALUES
      ('00E4M000000KG9hUAG'),
      ('00E4M000001IXLvUAO'),
      ('00E4M000001IXMPUA4'),
      ('00E4M000001IXMeUAO'),
      ('00E4M000001IXMjUAO'),
      ('00E4M000001IXMoUAO'),
      ('00E4M000001IXMtUAO'),
      ('00E4M000001IXN3UAO'),
      ('00E4M000001ReyiUAC'),
      ('00E8X000001vcDKUAY'),
      ('00E8X000001vcEQUAY'),
      ('00E8X000001vcEfUAI'),
      ('00EPL000001Aik12AC'),
      ('00EPL000001Aik32AC'),
      ('00EPL000001Aik52AC'),
      ('00EPL000001Aik82AC'),
      ('00EPL000001AikH2AS'),
      ('00EPL000001AikI2AS'),
      ('00EPL000001AikJ2AS'),
      ('00EPL000001AikL2AS'),
      ('00EPL000001AikN2AS'),
      ('00EPL000001AikO2AS'),
      ('00EPL000001AikP2AS'),
      ('00EPL000001AikQ2AS'),
      ('00EPL000001AikW2AS'),
      ('00EPL000001AikX2AS'),
      ('00EPL000001AikZ2AS'),
      ('00EPL000001Aikc2AC'),
      ('00EPL000001Aikd2AC'),
      ('00EPL000001Aike2AC'),
      ('00EPL000001Aikf2AC'),
      ('00EPL000001Aiki2AC'),
      ('00EPL000001Aikj2AC'),
      ('00EPL000001Aikl2AC'),
      ('00EPL000001Aikm2AC'),
      ('00EPL000001Aikn2AC'),
      ('00EPL000001Aiko2AC'),
      ('00EPL000001Aikp2AC'),
      ('00EPL000001Aikq2AC'),
      ('00EPL000001Aikr2AC'),
      ('00EPL000001AioO2AS'),
      ('00EPL000001AioP2AS'),
      ('00EPL000001AioQ2AS'),
      ('00EPL000001AioS2AS'),
      ('00EPL000001HsUu2AK'),
      ('00EPL000001Kr612AC'),
      ('00EPL000001NIba2AG'),
      ('00EPL000001NIbb2AG'),
      ('00EPL000001NIbd2AG'),
      ('00EPL000002SNzp2AG'),
      ('00EPL000004NGTB2A4'),
      ('00EPL000005pqGD2AY'),
      ('00EPL000005pqGH2AY'),
      ('00EPL000005pqGK2AY'),
      ('00EPL000005pqGL2AY'),
      ('00EPL000005pqGN2AY'),
      ('00EPL000005pqGR2AY'),
      ('00EPL000005pqGS2AY'),
      ('00EPL000005pqGT2AY'),
      ('00EPL000005pqGe2AI'),
      ('00EPL000005pqGf2AI'),
      ('00EPL000005pqGo2AI'),
      ('00EPL000005pqGp2AI'),
      ('00EPL000005pqGq2AI'),
      ('00EPL000005q3YP2AY'),
      ('00EPL000005q3a12AA'),
      ('00EPL000005qDW92AM'),
      ('00EPL000005qDWA2A2'),
      ('00EPL000005qDWE2A2'),
      ('00EPL000005qDWN2A2')
    ) AS geos (crm_user_role_id)
),

pubsec_user_group AS (
  SELECT 
    DISTINCT 
    dim_crm_user.dim_crm_user_id
  FROM {{ ref('sfdc_group_member_source') }} AS sfdc_group_member_source
    LEFT JOIN {{ ref('sfdc_group_source') }} AS sfdc_group_source 
      ON sfdc_group_member_source.group_id = sfdc_group_source.group_id
    LEFT JOIN {{ ref('dim_crm_user') }} AS dim_crm_user 
      ON sfdc_group_member_source.user_or_group_id = dim_crm_user.dim_crm_user_id
  WHERE sfdc_group_member_source.group_id IN (
    '00G4M000002KxljUAC', -- SAFE 2.0 - Pub Sec Hierarchy Exceptions 
    '00G8X000006HAyVUAW' -- SAFE 2.0 - PUBSEC
  )
),

sfdc_user_source AS (
  SELECT *
  FROM {{ ref('dim_crm_user') }}
),

tableau_group_source AS (
  SELECT *
  FROM {{ ref('tableau_cloud_groups_source') }}
),

sfdc_filtered AS (
  -- This CTE is for applying the user parameters that will later be used for application of geos.
  SELECT
    sfdc_user_source.dim_crm_user_id,
    sfdc_user_source.user_email,
    sfdc_user_source.crm_user_geo,
    sfdc_user_source.crm_user_sales_segment,
    sfdc_user_source.user_role_name,
    sfdc_user_source.user_role_id,
    IFF(target_geos.target_geos IS NOT NULL, TRUE, FALSE)        AS has_user_geo,
    IFF(target_user_roles.target_roles IS NOT NULL, TRUE, FALSE) AS has_global,
    IFF(target_user_roles.target_roles IS NOT NULL AND target_user_roles.has_pubsec = TRUE, TRUE, FALSE) AS has_pubsec,
    IFF(override_crm_users.crm_user_id IS NOT NULL, TRUE, FALSE) AS has_crm_user_override,
    IFF(override_pubsec_user_roles.crm_user_role_id IS NOT NULL, TRUE, FALSE) AS has_pubsec_user_role_override,
    IFF(smb_user_roles.crm_user_role_id IS NOT NULL, TRUE, FALSE) AS has_smb_user_role,
    IFF(pubsec_user_group.dim_crm_user_id IS NOT NULL, TRUE, FALSE) AS has_pubsec_user_group
  FROM sfdc_user_source
  LEFT JOIN target_geos
    ON LOWER(sfdc_user_source.crm_user_geo) = LOWER(target_geos.target_geos)
  LEFT JOIN target_user_roles
    ON LOWER(sfdc_user_source.user_role_name) = LOWER(target_user_roles.target_roles)
  LEFT JOIN override_crm_users
    ON sfdc_user_source.dim_crm_user_id = override_crm_users.crm_user_id
  LEFT JOIN override_pubsec_user_roles
    ON sfdc_user_source.user_role_id = override_pubsec_user_roles.crm_user_role_id
  LEFT JOIN smb_user_roles
    ON sfdc_user_source.user_role_id = smb_user_roles.crm_user_role_id
  LEFT JOIN pubsec_user_group
    ON sfdc_user_source.dim_crm_user_id = pubsec_user_group.dim_crm_user_id
  WHERE sfdc_user_source.is_active = TRUE
),

tableau_safe_users AS (
  -- This CTE is for collecting a the list of SAFE Tableau users from the primary Tableau site.
  SELECT user_email
  FROM tableau_group_source
  WHERE site_luid = '2ae725b5-5a7b-40ba-a05c-35b7aa9ab731' -- Main Tableau Site
    AND group_name = 'General SAFE Access'
    AND group_luid = 'e926984d-06de-4536-b572-6d09075a21be' -- General SAFE Access
),

geo_users AS (
  -- This CTE is for filtering to a list of Salesforce users that have an assigned geo.
  SELECT
    dim_crm_user_id,
    crm_user_geo AS crm_geo,
    crm_user_sales_segment,
    IFF(LOWER(crm_user_sales_segment) = 'pubsec', CONCAT(crm_user_geo, '-', UPPER(crm_user_sales_segment)), crm_user_geo) AS geo_pubsec_segment,
    user_email,
    'sfdc_user_geo' AS entitlement_basis
  FROM sfdc_filtered
  WHERE has_user_geo = TRUE
    AND has_crm_user_override = FALSE
    AND has_global = FALSE
),

all_accounts AS (
  -- This CTE is for filtering to a list of Salesforce users that have no assigned geo but 
  -- have SAFE access and one of the target Salesforce user roles
  SELECT
    geo_list.crm_user_geo AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    geo_list.crm_user_geo AS geo_pubsec_segment,
    sfdc_filtered.user_email,
    'tableau_safe_and_sfdc_role' AS entitlement_basis
  FROM sfdc_filtered
  INNER JOIN tableau_safe_users
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  LEFT JOIN geo_users
    ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
  CROSS JOIN (
    SELECT DISTINCT crm_user_geo
    FROM sfdc_filtered
    WHERE crm_user_geo IS NOT NULL
      AND has_user_geo = TRUE 
  ) AS geo_list
  WHERE geo_users.user_email IS NULL
    AND sfdc_filtered.has_global = TRUE
    AND sfdc_filtered.has_pubsec = TRUE
    AND sfdc_filtered.has_crm_user_override = FALSE

  UNION ALL
  
  -- Additional AMER-PUBSEC access
  SELECT
    'AMER' AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    'AMER-PUBSEC' AS geo_pubsec_segment,
    sfdc_filtered.user_email,
    'tableau_safe_and_sfdc_role' AS entitlement_basis
  FROM sfdc_filtered
  INNER JOIN tableau_safe_users
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  LEFT JOIN geo_users
    ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
  WHERE geo_users.user_email IS NULL
    AND sfdc_filtered.has_global = TRUE
    AND sfdc_filtered.has_pubsec = TRUE
    AND sfdc_filtered.has_crm_user_override = FALSE
),

non_pubsec AS (
  -- This CTE if for filtering to a list of Salesforce users that have no assigned geo but 
  -- have SAFE access, one of the target Salesforce user roles, but do not have access the PUBSEC geo
  SELECT
    geo_list.crm_user_geo AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    geo_list.crm_user_geo AS geo_pubsec_segment,
    sfdc_filtered.user_email,
    'tableau_safe_and_sfdc_role_less_pubsec' AS entitlement_basis
  FROM sfdc_filtered
  INNER JOIN tableau_safe_users
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  LEFT JOIN geo_users
    ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
  CROSS JOIN (
    SELECT DISTINCT crm_user_geo
    FROM sfdc_filtered
    WHERE crm_user_geo IS NOT NULL
      AND has_user_geo = TRUE 
  ) AS geo_list
  WHERE geo_users.user_email IS NULL
    AND sfdc_filtered.has_pubsec = FALSE
    AND sfdc_filtered.has_global = TRUE
    AND LOWER(geo_list.crm_user_geo) != LOWER('PUBSEC')
    AND sfdc_filtered.has_crm_user_override = FALSE
),

channel_sfdc_users AS (
  SELECT
    sfdc_filtered.user_email,
    'CHANNEL' AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    'CHANNEL' AS geo_pubsec_segment,
    'all_sfdc_users' AS entitlement_basis
  FROM sfdc_filtered
  WHERE sfdc_filtered.has_crm_user_override = FALSE
),

non_sfdc_safe AS (
  -- This CTE if for filtering to a list of Tableau that have SAFE access 
  -- but not Salesforce access.
  SELECT
    target_geos.target_geos AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    target_geos.target_geos AS geo_pubsec_segment,
    tableau_safe_users.user_email,
    'tableau_safe' AS entitlement_basis
  FROM tableau_safe_users
  LEFT JOIN sfdc_filtered
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  CROSS JOIN target_geos
  WHERE sfdc_filtered.user_email IS NULL
    AND target_geos.target_geos <> 'AMER'  -- Exclude AMER as it's handled separately

  UNION ALL
  
  -- AMER access
  SELECT
    'AMER' AS crm_geo,
    NULL AS crm_user_sales_segment,
    'AMER' AS geo_pubsec_segment,
    tableau_safe_users.user_email,
    'tableau_safe' AS entitlement_basis
  FROM tableau_safe_users
  LEFT JOIN sfdc_filtered
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  WHERE sfdc_filtered.user_email IS NULL

  UNION ALL
  
  -- Additional AMER-PUBSEC access
  SELECT
    'AMER' AS crm_geo,
    NULL AS crm_user_sales_segment,
    'AMER-PUBSEC' AS geo_pubsec_segment,
    tableau_safe_users.user_email,
    'tableau_safe' AS entitlement_basis
  FROM tableau_safe_users
  LEFT JOIN sfdc_filtered
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  WHERE sfdc_filtered.user_email IS NULL
),

pubsec_override AS (
  SELECT
    sfdc_filtered.user_email,
    sfdc_filtered.crm_user_geo AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    CONCAT(sfdc_filtered.crm_user_geo, '-PUBSEC') AS geo_pubsec_segment,
    'pubsec_override' AS entitlement_basis
  FROM sfdc_filtered
  WHERE sfdc_filtered.has_pubsec_user_role_override = TRUE
),

smb_access AS (
  SELECT
    'SMB' AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    'SMB' AS geo_pubsec_segment,
    sfdc_filtered.user_email,
    'smb_user_role' AS entitlement_basis
  FROM sfdc_filtered
  WHERE  sfdc_filtered.has_smb_user_role = TRUE    
),

pubsec_user_group_override AS (
  SELECT
    sfdc_filtered.user_email,
    'AMER' AS crm_geo,      -- Set to AMER 
    sfdc_filtered.crm_user_sales_segment,
    'AMER-PUBSEC' AS geo_pubsec_segment,  -- Set to AMER-PUBSEC
    'pubsec_user_group_override' AS entitlement_basis
  FROM sfdc_filtered
  WHERE sfdc_filtered.has_pubsec_user_group = TRUE    
),

crm_user_override AS (
  SELECT
    sfdc_filtered.user_email,
    IFF(target_geos.target_geos = 'PUBSEC', 'AMER', target_geos.target_geos) AS crm_geo,
    sfdc_filtered.crm_user_sales_segment,
    IFF(target_geos.target_geos = 'PUBSEC', 'AMER-PUBSEC', target_geos.target_geos) AS geo_pubsec_segment,
    'crm_user_override' AS entitlement_basis
  FROM sfdc_filtered
  CROSS JOIN target_geos
  WHERE sfdc_filtered.has_crm_user_override = TRUE
),

combined AS (
  SELECT 
    crm_geo,
    crm_user_sales_segment,
    geo_pubsec_segment,
    user_email,
    entitlement_basis
  FROM non_pubsec

  UNION ALL

  SELECT 
    crm_geo,
    crm_user_sales_segment,
    geo_pubsec_segment,
    user_email,
    entitlement_basis
  FROM all_accounts

  UNION ALL

  SELECT DISTINCT
    geo_users.crm_geo,
    geo_users.crm_user_sales_segment,
    geo_users.geo_pubsec_segment,
    geo_users.user_email,
    entitlement_basis
  FROM geo_users

  UNION ALL

  SELECT DISTINCT
    channel_sfdc_users.crm_geo,
    channel_sfdc_users.crm_user_sales_segment,
    channel_sfdc_users.geo_pubsec_segment,
    channel_sfdc_users.user_email,
    entitlement_basis
  FROM channel_sfdc_users

  UNION ALL

  SELECT DISTINCT
    non_sfdc_safe.crm_geo,
    non_sfdc_safe.crm_user_sales_segment,
    non_sfdc_safe.geo_pubsec_segment,
    non_sfdc_safe.user_email,
    entitlement_basis
  FROM non_sfdc_safe

  UNION ALL

  SELECT DISTINCT
    pubsec_override.crm_geo,
    pubsec_override.crm_user_sales_segment,
    pubsec_override.geo_pubsec_segment,
    pubsec_override.user_email,
    entitlement_basis
  FROM pubsec_override
  
  UNION ALL
  
  SELECT DISTINCT
    smb_access.crm_geo,
    smb_access.crm_user_sales_segment,
    smb_access.geo_pubsec_segment,
    smb_access.user_email,
    entitlement_basis
  FROM smb_access

  UNION ALL

  SELECT DISTINCT
    pubsec_user_group_override.crm_geo,
    pubsec_user_group_override.crm_user_sales_segment,
    pubsec_user_group_override.geo_pubsec_segment,
    pubsec_user_group_override.user_email,
    entitlement_basis
  FROM pubsec_user_group_override

  UNION ALL

  SELECT DISTINCT
    crm_user_override.crm_geo,
    crm_user_override.crm_user_sales_segment,
    crm_user_override.geo_pubsec_segment,
    crm_user_override.user_email,
    entitlement_basis
  FROM crm_user_override
)

SELECT 
    user_email,
    geo_pubsec_segment,
    LISTAGG(DISTINCT entitlement_basis, ', ') AS entitlement_basis
FROM combined
GROUP BY 1, 2
