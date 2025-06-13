WITH team_yaml_historical AS (

  SELECT *
  FROM {{ ref('team_yaml_historical') }}

),

date_details AS (

  SELECT *
  FROM {{ ref('prep_date') }}

),

yaml_counts AS (

  SELECT
    snapshot_date,
    SUM(
        CASE 
            WHEN ARRAY_CONTAINS('maintainer backend'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('maintainer backend'::variant, PARSE_JSON(projects):"gitlab"::array)
                THEN 1 ELSE 0 END) AS backend_maintainer,
    SUM(CASE
            WHEN ARRAY_CONTAINS('maintainer frontend'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('maintainer frontend'::variant, PARSE_JSON(projects):"gitlab"::array) 
                THEN 1 ELSE 0 END) AS frontend_maintainer,
    SUM(CASE 
            WHEN ARRAY_CONTAINS('maintainer database'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('maintainer database'::variant, PARSE_JSON(projects):"gitlab"::array) 
                THEN 1 ELSE 0 END) AS database_maintainer,
    SUM(CASE
            WHEN ARRAY_CONTAINS('trainee_maintainer backend'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('trainee_maintainer backend'::variant, PARSE_JSON(projects):"gitlab"::array) 
                THEN 1 ELSE 0 end) AS backend_trainee,
    SUM(CASE 
            WHEN ARRAY_CONTAINS('trainee_maintainer frontend'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('trainee_maintainer frontend'::variant, PARSE_JSON(projects):"gitlab"::array) 
                THEN 1 else 0 end) AS frontend_trainee,
    SUM(CASE 
            WHEN ARRAY_CONTAINS('trainee_maintainer database'::variant, PARSE_JSON(projects):"gitlab-ce"::array) OR ARRAY_CONTAINS('trainee_maintainer database'::variant, PARSE_JSON(projects):"gitlab"::array) 
                THEN 1 ELSE 0 END) AS database_trainee
  FROM team_yaml_historical
  GROUP BY 1

),

engineers AS (

  SELECT
    date_actual,
    COUNT(DISTINCT
        CASE
            WHEN (position LIKE '%Backend Engineer%' 
            OR position LIKE '%Fullstack Engineer%' 
            OR position LIKE 'Distinguished Engineer%'
            OR position = 'Engineering Fellow')
                THEN employee_id ELSE NULL END) AS backend,
    COUNT(DISTINCT
        CASE 
            WHEN position LIKE '%Frontend Engineer%' 
                THEN employee_Id ELSE NULL END) AS frontend
  FROM {{ ref('team_member_history') }}
  WHERE Department != 'Meltano'
  GROUP BY 1
  HAVING backend > 0 OR frontend > 0

),

final AS (

  SELECT
    date_details.date_actual AS date,
    engineers.backend,
    engineers.frontend,
    yaml_counts.backend_maintainer,
    yaml_counts.frontend_maintainer,
    yaml_counts.database_maintainer,
    yaml_counts.backend_trainee,
    yaml_counts.frontend_trainee,
    yaml_counts.database_trainee,
    CAST(Engineers.backend AS float) / backend_maintainer AS engineers_per_maintainer_backend,
    CAST(Engineers.frontend AS float) / frontend_maintainer AS engineers_per_maintainer_frontend,
    CAST(engineers.backend AS float) / backend_trainee AS engineers_per_trainee_backend,
    CAST(engineers.frontend AS float) / frontend_trainee AS engineers_per_trainee_frontend
  FROM date_details
  LEFT JOIN engineers ON date_details.date_actual = engineers.date_actual
  LEFT JOIN yaml_counts ON date_details.date_actual = yaml_counts.snapshot_date
  WHERE date_details.date_actual BETWEEN dateadd('year',-4,current_date) AND current_date

)

SELECT *
FROM final
