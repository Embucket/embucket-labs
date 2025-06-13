WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'clari_export_forecast_net_iacv') }}

), renamed AS (

    SELECT
         "User"::VARCHAR            AS user,
         "Email"::VARCHAR           AS email,
         "CRM_User_ID"::VARCHAR     AS crm_user_id,
         "Role"::VARCHAR            AS sales_team_role,
         "Parent_Role"::VARCHAR     AS parent_role,
         "Timeframe"::VARCHAR       AS timeframe,
         "Field"::VARCHAR           AS field,
         "Week"::NUMBER             AS week,
         "Start_Day"::DATE          AS start_date,
         "End_Day"::DATE            AS end_date,
         "Data_Type"::VARCHAR       AS data_type,
         "Data_Value"::VARCHAR      AS data_value
    FROM source

)

SELECT *
FROM renamed
