WITH sfdc_account AS (

    SELECT 
      {{ dbt_utils.star(from=ref('sfdc_account'), except=["ACCOUNT_OWNER_USER_SEGMENT"])}}
    FROM {{ ref('sfdc_account') }}

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}     

), sfdc_account_deal_size_segmentation AS (

    SELECT *
    FROM {{ ref('sfdc_account_deal_size_segmentation') }}

), joined AS (


    SELECT
      sfdc_account.*,
      tam_user.name                                                                   AS technical_account_manager,
      sfdc_account.ultimate_parent_sales_segment                                      AS ultimate_parent_account_segment,
      sfdc_record_type.record_type_name,
      sfdc_record_type.business_process_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_description,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_account_deal_size_segmentation.deal_size,
      CASE 
        WHEN sfdc_account.ultimate_parent_sales_segment IN ('Large', 'Strategic')
          OR sfdc_account.division_sales_segment IN ('Large', 'Strategic') 
          THEN TRUE
        ELSE FALSE 
      END                                                                               AS is_large_and_up,


      -- NF 20210829 Zoom info technology flags
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies,'ARE_USED: Jenkins') 
          THEN 1 
        ELSE 0
      END                                 AS zi_jenkins_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: SVN') 
          THEN 1 
        ELSE 0
      END                                 AS zi_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Tortoise SVN') 
          THEN 1 
        ELSE 0
      END                                 AS zi_tortoise_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Google Cloud Platform') 
          THEN 1 
        ELSE 0
      END                                 AS zi_gcp_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Atlassian') 
          THEN 1 
        ELSE 0
      END                                 AS zi_atlassian_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub') 
          THEN 1 
        ELSE 0
      END                                 AS zi_github_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub Enterprise') 
          THEN 1 
        ELSE 0
      END                                 AS zi_github_enterprise_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: AWS') 
          THEN 1 
        ELSE 0
      END                                 AS zi_aws_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Kubernetes') 
          THEN 1 
        ELSE 0
      END                                 AS zi_kubernetes_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion') 
          THEN 1 
        ELSE 0
      END                                 AS zi_apache_subversion_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion (SVN)') 
          THEN 1 
        ELSE 0
      END                                 AS zi_apache_subversion_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Hashicorp') 
          THEN 1 
        ELSE 0
      END                                 AS zi_hashicorp_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Amazon AWS CloudTrail') 
          THEN 1 
        ELSE 0
      END                                 AS zi_aws_cloud_trail_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: CircleCI') 
          THEN 1 
        ELSE 0
      END                                 AS zi_circle_ci_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: BitBucket') 
          THEN 1 
        ELSE 0
      END                                 AS zi_bit_bucket_presence_flag,

    -- NF 2022-01-28 Added extra account owner demographics fields
    --account_owner.user_segment            AS account_owner_user_segment, -- coming directly from source table
    -- JK: 2022-11-16 changing the source back to the user object to align with EDM 
    account_owner.user_segment            AS account_owner_user_segment,
    account_owner.user_geo                AS account_owner_user_geo, 
    account_owner.user_region             AS account_owner_user_region,
    account_owner.user_area               AS account_owner_user_area,

    sfdc_account.account_sales_segment    AS parent_crm_account_sales_segment,
    sfdc_account.account_geo              AS parent_crm_account_geo,
    sfdc_account.account_region           AS parent_crm_account_region,
    sfdc_account.account_area             AS parent_crm_account_area,
    sfdc_account.account_territory        AS parent_crm_account_territory

    FROM sfdc_account
    LEFT JOIN sfdc_users tam_user
      ON sfdc_account.technical_account_manager_id = tam_user.user_id
    LEFT JOIN sfdc_users account_owner
      ON sfdc_account.owner_id = account_owner.user_id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN sfdc_account_deal_size_segmentation
      ON sfdc_account.account_id = sfdc_account_deal_size_segmentation.account_id

)

SELECT *
FROM joined
