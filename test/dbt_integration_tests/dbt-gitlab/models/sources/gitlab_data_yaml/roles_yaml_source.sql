WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'roles') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), intermediate_role_information AS (

    SELECT
      snapshot_date,
      data_by_row['title']::VARCHAR           AS title,
      data_by_row['levels']::VARCHAR          AS role_levels,
      data_by_row['open']::VARCHAR            AS is_open,
      data_by_row['salary']::VARCHAR          AS previous_salary_value,
      data_by_row['ic_ttc']                   AS ic_values,
      data_by_row['manager_ttc']              AS manager_values,
      data_by_row['director_ttc']             AS director_values,
      data_by_row['senior_director_ttc']      AS senior_director_values,
      rank
    FROM intermediate

), renamed AS (

    SELECT
      snapshot_date,
      title,
      role_levels,
      TRY_TO_BOOLEAN(is_open)                                                             AS is_open,
      TRY_TO_NUMERIC(previous_salary_value)                                               AS previous_salary_value,
      TRY_TO_NUMERIC(ic_values['compensation']::VARCHAR)                                  AS individual_contributor_compensation,
      TRY_TO_NUMERIC(ic_values['percentage_variable']::VARCHAR,5,2)                       AS individual_contributor_percentage_variable,
      TRY_TO_BOOLEAN(ic_values['from_base']::VARCHAR)                                     AS individual_contributor_from_base,
      TRY_TO_NUMERIC(manager_values['compensation']::VARCHAR)                             AS manager_compensation,
      TRY_TO_NUMERIC(manager_values['percentage_variable']::VARCHAR)                      AS manager_percentage_variable,
      TRY_TO_BOOLEAN(manager_values['from_base']::VARCHAR)                                AS manager_from_base,
      TRY_TO_NUMERIC(director_values['compensation']::VARCHAR)                            AS director_compensation,
      TRY_TO_NUMERIC(director_values['percentage_variable']::VARCHAR,5,2)                 AS director_percentage_variable,
      TRY_TO_BOOLEAN(director_values['from_base']::VARCHAR)                               AS director_from_base,
      TRY_TO_NUMERIC(senior_director_values['compensation']::VARCHAR)                     AS senior_director_compensation,
      TRY_TO_NUMERIC(senior_director_values['percentage_variable']::VARCHAR,5,2)          AS senior_director_percentage_variable,
      TRY_TO_BOOLEAN(senior_director_values['from_base']::VARCHAR)                        AS senior_director_from_base,
      rank
    FROM intermediate_role_information

)

SELECT *
FROM renamed