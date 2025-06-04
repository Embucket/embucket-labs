WITH source AS (

    SELECT *
    FROM {{ source('salesforce_sandbox', 'user') }}

), renamed AS(

    SELECT

      -- ids
      id                                                                AS user_id,
      name                                                              AS name,
      email                                                             AS user_email,
      employeenumber                                                    AS employee_number,

      -- info
      title                                                             AS title,
      team__c                                                           AS team,
      department                                                        AS department,
      managerid                                                         AS manager_id,
      manager_name__c                                                   AS manager_name,
      isactive                                                          AS is_active,
      userroleid                                                        AS user_role_id,
      user_role_type__c                                                 AS user_role_type,
      role_level_1__c                                                   AS user_role_level_1,
      role_level_2__c                                                   AS user_role_level_2,
      role_level_3__c                                                   AS user_role_level_3,
      role_level_4__c                                                   AS user_role_level_4,
      role_level_5__c                                                   AS user_role_level_5,
      start_date__c                                                     AS start_date,
      {{ sales_hierarchy_sales_segment_cleaning('user_segment__c') }}   AS user_segment,
      user_geo__c                                                       AS user_geo,
      user_region__c                                                    AS user_region,
      user_area__c                                                      AS user_area,
      user_business_unit__c                                             AS user_business_unit,
      user_segment_geo_region_area__c                                   AS user_segment_geo_region_area,
      timezonesidkey                                                    AS user_timezone,
      CASE
        WHEN user_segment IN ('Large', 'PubSec') THEN 'Large'
        ELSE user_segment
      END                                                               AS user_segment_grouped,
      {{ sales_segment_region_grouped('user_segment', 'user_geo', 'user_region') }}
                                                                        AS user_segment_region_grouped,
      true::BOOLEAN                                                AS is_hybrid_user,

      --metadata
      createdbyid                                                       AS created_by_id,
      createddate                                                       AS created_date,
      lastmodifiedbyid                                                  AS last_modified_id,
      lastmodifieddate                                                  AS last_modified_date,
      systemmodstamp,

      --dbt last run
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run

    FROM source

)

SELECT *
FROM renamed
