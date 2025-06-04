WITH source AS (
  
    SELECT * 
    FROM {{ source('hyperproof', 'programs') }}
    
), metric_per_row AS (

    SELECT 
      data_by_row.value['baselineEnabled']::VARCHAR                 AS baseline_enabled,
      data_by_row.value['clonedProgramId']::VARCHAR                 AS cloned_program_id,
      data_by_row.value['createdBy']::VARCHAR                       AS created_by,
      data_by_row.value['createdOn']::TIMESTAMP                     AS created_on,
      data_by_row.value['description']::VARCHAR                     AS description,
      data_by_row.value['frameworkLicenseNotice']::VARCHAR          AS framework_license_notice,
      data_by_row.value['id']::VARCHAR                              AS id,
      data_by_row.value['isScoringEnabled']::VARCHAR                AS is_scoring_enabled,
      data_by_row.value['latestUpdatedToProgramId']::VARCHAR        AS latest_updated_to_programId,
      data_by_row.value['name']::VARCHAR                            AS name,
      data_by_row.value['orgId']::VARCHAR                           AS org_id,
      data_by_row.value['overrideHealth']::VARCHAR                  AS override_health,
      data_by_row.value['permissions']::VARCHAR                     AS permissions,
      data_by_row.value['primaryContactId']::VARCHAR                AS primary_contact_id,
      data_by_row.value['sectionRootId']::VARCHAR                   AS section_root_id,
      data_by_row.value['selectedBaselines']::VARCHAR               AS selected_baselines,
      data_by_row.value['sourceFrameworkId']::VARCHAR               AS source_framework_id,
      data_by_row.value['sourceTemplateId']::VARCHAR                AS source_template_id,
      data_by_row.value['status']::VARCHAR                          AS status,
      data_by_row.value['updatedBy']::VARCHAR                       AS updated_by,
      data_by_row.value['updatedOn']::TIMESTAMP                     AS updated_on,
      data_by_row.value['workStatus']::VARCHAR                      AS work_status,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row

)
SELECT *
FROM metric_per_row
