WITH mapping as (

    SELECT *
    FROM {{ref('bamboohr_id_employee_number_mapping')}}
    --mapping on bamboohr_id_employee_number_mapping as this has accounted for all hired employees whereas bamboohr_directory_source has not

), bamboohr_directory AS (

    SELECT *
    FROM {{ ref('blended_directory_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, DATE_TRUNC(day, uploaded_at) ORDER BY uploaded_at DESC) = 1

), intermediate AS (

    SELECT bamboohr_directory.*,
      LAST_VALUE(DATE_TRUNC(DAY, bamboohr_directory.uploaded_at)) OVER 
            (PARTITION BY bamboohr_directory.employee_id ORDER BY uploaded_at)                         AS max_uploaded_date,
      DENSE_RANK() OVER (PARTITION BY bamboohr_directory.employee_id ORDER BY uploaded_at DESC)        AS rank_email_desc        
    FROM mapping
    LEFT JOIN bamboohr_directory
      ON mapping.employee_id = bamboohr_directory.employee_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bamboohr_directory.employee_id, work_email ORDER BY uploaded_at) = 1       

), final AS (

    SELECT
      employee_id,
      full_name,
      work_email,
      uploaded_at                                                                     AS valid_from_date,
      IFF(max_uploaded_date< CURRENT_DATE() AND rank_email_desc = 1, 
        max_uploaded_date, 
        COALESCE(LEAD(DATEADD(day,-1,uploaded_at)) OVER (PARTITION BY employee_id ORDER BY uploaded_at),
                {{max_date_in_bamboo_analyses()}}))                                   AS valid_to_date,
      DENSE_RANK() OVER (PARTITION BY employee_id ORDER BY valid_from_date DESC)      AS rank_email_desc
    FROM intermediate
)

SELECT *
FROM final