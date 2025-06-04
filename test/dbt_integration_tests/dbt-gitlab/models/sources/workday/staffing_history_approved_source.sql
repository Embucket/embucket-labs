WITH source AS (

  SELECT *
  FROM {{ source('workday','staffing_history_approved') }}
  WHERE NOT _fivetran_deleted

),

renamed AS (

  SELECT
    source.employee_id::NUMBER                                          AS employee_id,
    d.value['WORKDAY_ID']::VARCHAR                                      AS workday_id,
    d.value['BUSINESS_PROCESS_TYPE']::VARCHAR                           AS business_process_type,
    d.value['BUSINESS_PROCESS_CATEGORY']::VARCHAR                       AS business_process_category,
    d.value['BUSINESS_PROCESS_REASON']::VARCHAR                         AS business_process_reason,
    d.value['HIRE_DATE']::DATE                                          AS hire_date,
    d.value['TERMINATION_DATE']::DATE                                   AS termination_date,
    d.value['COUNTRY_CURRENT']::VARCHAR                                 AS country_past,
    d.value['COUNTRY_PROPOSED']::VARCHAR                                AS country_current,
    d.value['REGION_CURRENT']::VARCHAR                                  AS region_past,
    d.value['REGION_PROPOSED']::VARCHAR                                 AS region_current,
    d.value['DEPARTMENT_ID_CURRENT']::VARCHAR                           AS department_workday_id_past,
    d.value['DEPARTMENT_ID_PROPOSED']::VARCHAR                          AS department_workday_id_current,
    d.value['DEPARTMENT_CURRENT']::VARCHAR                              AS department_past,
    d.value['DEPARTMENT_PROPOSED']::VARCHAR                             AS department_current,
    d.value['EMPL_TYPE_CURRENT']::VARCHAR                               AS employee_type_past,
    d.value['EMPL_TYPE_PROPOSED']::VARCHAR                              AS employee_type_current,
    d.value['ENTITY_CURRENT']::VARCHAR                                  AS entity_past,
    d.value['ENTITY_PROPOSED']::VARCHAR                                 AS entity_current,
    d.value['JOB_CODE_CURRENT']::VARCHAR                                AS job_code_past,
    d.value['JOB_CODE_PROPOSED']::VARCHAR                               AS job_code_current,
    d.value['JOB_SPECIALTY_MULTI_CURRENT']::VARCHAR                     AS job_specialty_multi_past,
    d.value['JOB_SPECIALTY_MULTI_PROPOSED']::VARCHAR                    AS job_specialty_multi_current,
    d.value['JOB_SPECIALTY_SINGLE_CURRENT']::VARCHAR                    AS job_specialty_single_past,
    d.value['JOB_SPECIALTY_SINGLE_PROPOSED']::VARCHAR                   AS job_specialty_single_current,
    d.value['LOCALITY_CURRENT']::VARCHAR                                AS locality_past,
    d.value['LOCALITY_PROPOSED']::VARCHAR                               AS locality_current,
    d.value['MANAGER_CURRENT']::VARCHAR                                 AS manager_past,
    d.value['MANAGER_PROPOSED']::VARCHAR                                AS manager_current,
    d.value['SUPORG_CURRENT']::VARCHAR                                  AS suporg_past,
    d.value['SUPORG_PROPOSED']::VARCHAR                                 AS suporg_current,
    d.value['TEAM_ID_CURRENT']::VARCHAR                                 AS team_id_past,
    d.value['TEAM_ID_PROPOSED']::VARCHAR                                AS team_id_current,
    d.value['TEAM_WORKDAY_ID_CURRENT']::VARCHAR                         AS team_workday_id_past,
    d.value['TEAM_WORKDAY_ID_PROPOSED']::VARCHAR                        AS team_workday_id_current,
    d.value['JOB_WORKDAY_ID_CURRENT']::VARCHAR                          AS job_workday_id_past,
    d.value['JOB_WORKDAY_ID_PROPOSED']::VARCHAR                         AS job_workday_id_current,
    d.value['JOB_TITLE_CURRENT']::VARCHAR                               AS job_title_past,
    d.value['JOB_TITLE_PROPOSED']::VARCHAR                              AS job_title_current,
    d.value['JOB_TITLE_PROPOSED_IS_JOB_PROFILE_PROPOSED']::BOOLEAN      AS does_job_title_current_match_profile,
    d.value['BUSINESS_TITLE_CURRENT']::VARCHAR                          AS business_title_past,
    d.value['BUSINESS_TITLE_PROPOSED']::VARCHAR                         AS business_title_current,
    d.value['BUSINESS_TITLE_PROPOSED_IS_JOB_PROFILE_PROPOSED']::BOOLEAN AS does_business_title_current_match_profile,
    d.value['DATE_TIME_INITIATED']::TIMESTAMP                           AS date_time_initiated,
    d.value['EFFECTIVE_DATE']::DATE                                     AS effective_date,
    IFF(business_process_type IN (
		  'Change Organization Assignments for Worker',
      'Move Worker (By Organization)'
		),0,1)                                                              AS process_rank
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(staffing_history_approved)) AS d

), 

final AS (

SELECT * EXCLUDE(process_rank)
FROM renamed
WHERE effective_date <= CURRENT_DATE()
QUALIFY ROW_NUMBER() OVER(PARTITION BY employee_id, effective_date, date_time_initiated ORDER BY process_rank DESC) = 1

)

SELECT *
FROM final
