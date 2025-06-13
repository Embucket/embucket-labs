WITH mapping_table AS (

    SELECT *
    FROM {{ ref('sheetload_mapping_sdr_sfdc_bamboohr_source') }}

), sfdc_users AS (

    SELECT
      *,
      substr(user_id, 1, 15)         AS trim_mapping
    FROM {{ ref('sfdc_users_source')}}

)

SELECT
  sfdc_users.user_id,
  mapping_table.user_id              AS fifteen_length_user_id,
  mapping_table.first_name,
  mapping_table.last_name,
  mapping_table.username,
  mapping_table.active,
  mapping_table.profile,
  mapping_table.eeid,
  mapping_table.sdr_segment,
  mapping_table.sdr_region,
  mapping_table.sdr_order_type

FROM mapping_table
LEFT JOIN sfdc_users
  ON mapping_table.user_id = sfdc_users.trim_mapping
