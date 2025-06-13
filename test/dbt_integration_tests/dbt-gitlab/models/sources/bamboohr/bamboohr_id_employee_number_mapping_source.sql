WITH source AS (
    
    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(day, uploaded_at) ORDER BY uploaded_at DESC) = 1 

), intermediate AS (

    SELECT 
      NULLIF(d.value['employeeNumber'],'')::NUMBER                    AS employee_number,
      d.value['id']::NUMBER                                           AS employee_id,
      d.value['firstName']::VARCHAR                                   AS first_name,
      d.value['lastName']::VARCHAR                                    AS last_name,
      (CASE WHEN d.value['hireDate']=''
            THEN NULL
           WHEN d.value['hireDate']= '0000-00-00'
            THEN NULL
           ELSE d.value['hireDate']::VARCHAR END)::DATE               AS hire_date,
      (CASE WHEN d.value['terminationDate']=''
            THEN NULL
           WHEN d.value['terminationDate']= '0000-00-00'
            THEN NULL
           ELSE d.value['terminationDate']::VARCHAR END)::DATE        AS termination_date,
      d.value['customCandidateID']::NUMBER                            AS greenhouse_candidate_id,
      d.value['customCostCenter']::VARCHAR                            AS cost_center,
      d.value['customGitLabUsername']::VARCHAR                        AS gitlab_username,
      d.value['customJobTitleSpeciality']::VARCHAR                    AS jobtitle_speciality_single_select,
      d.value['customJobTitleSpecialty(Multi-Select)']::VARCHAR       AS jobtitle_speciality_multi_select,
      -- requiers cleaning becase of an error in the snapshoted source data
      CASE d.value['customLocality']::VARCHAR
        WHEN 'Canberra, Australia Capital Territory, Australia'
            THEN 'Canberra, Australian Capital Territory, Australia'
        ELSE d.value['customLocality']::VARCHAR
      END                                                             AS locality,
      d.value['customNationality']::VARCHAR                           AS nationality,
      d.value['customOtherGenderOptions']::VARCHAR                    AS gender_dropdown, 
      d.value['customRegion']::VARCHAR                                AS region,
      d.value['customRole']::VARCHAR                                  AS job_role,
      d.value['customSalesGeoDifferential']::VARCHAR                  AS sales_geo_differential,
      d.value['dateofBirth']::VARCHAR                                 AS date_of_birth,
      d.value['employeeStatusDate']::VARCHAR                          AS employee_status_date,
      d.value['employmentHistoryStatus']::VARCHAR                     AS employment_history_status,
      d.value['ethnicity']::VARCHAR                                   AS ethnicity,
      d.value['gender']::VARCHAR                                      AS gender, 
      TRIM(d.value['country']::VARCHAR)                               AS country,
      d.value['age']::NUMBER                                          AS age,
      COALESCE(d.value['customJobGrade']::VARCHAR,
                d.value['4659.0']::VARCHAR)                           AS job_grade,
      COALESCE(d.value['customPayFrequency']::VARCHAR,
               d.value['4657.0']::VARCHAR)                            AS pay_frequency,
      uploaded_at::TIMESTAMP                                          AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['employees']), OUTER => true) d

), final AS (

    SELECT *,
      DENSE_RANK() OVER (ORDER BY uploaded_at DESC)                   AS uploaded_row_number_desc
    FROM intermediate 
    WHERE hire_date IS NOT NULL
      AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
      AND LOWER(last_name) NOT LIKE '%test profile%'
      AND LOWER(last_name) != 'test-gitlab')
    -- The same emplpyee can appear more than once in the same upload.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_number, DATE_TRUNC(day, uploaded_at) ORDER BY uploaded_at DESC) = 1

) 



SELECT * 
FROM final