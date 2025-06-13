WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','leading_organization_entry_dates') }}
    
),

renamed AS (

  SELECT
    contributor_organization::VARCHAR AS contributor_organization,
    true::BOOLEAN AS is_individual_contributor,
    date_of_entry_in_program::DATE AS entry_in_program_date,
    -- date_of_exit_of_program was empty on first load and was read in as a FLOAT
    date_of_exit_of_program::VARCHAR::DATE AS exit_of_program_date
  FROM source

)

SELECT *
FROM renamed
