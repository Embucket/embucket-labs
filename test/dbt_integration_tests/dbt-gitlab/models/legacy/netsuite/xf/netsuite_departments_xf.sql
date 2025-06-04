WITH base_departments AS (

    SELECT *
    FROM {{ ref('netsuite_departments_source') }}

), parent_department AS (

    SELECT
      a.department_id,
      a.department_name,
      a.department_full_name,
      a.is_department_inactive,
      CASE WHEN a.parent_department_id IS NOT NULL THEN a.parent_department_id
           ELSE a.department_id
      END                               AS parent_department_id,
      CASE WHEN a.parent_department_id IS NOT NULL THEN b.department_name
           ELSE a.department_name
      END                               AS parent_department_name
    FROM base_departments a
    LEFT JOIN base_departments b
      ON a.parent_department_id = b.department_id

)

SELECT *
FROM parent_department
