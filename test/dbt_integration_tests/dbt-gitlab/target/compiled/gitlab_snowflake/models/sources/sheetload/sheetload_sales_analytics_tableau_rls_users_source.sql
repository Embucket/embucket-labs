WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sales_analytics_tableau_rls_users

), renamed AS (

    SELECT
      email::VARCHAR                              AS email,
      username::VARCHAR                           AS username,
      role::VARCHAR                               AS role

    FROM source

)

SELECT *
FROM renamed