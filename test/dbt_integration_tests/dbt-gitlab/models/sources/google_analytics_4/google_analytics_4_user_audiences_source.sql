WITH source AS (

  SELECT *
  FROM {{ source('google_analytics_4_bigquery','pseudonymous_users') }}

),

flattened AS (


  SELECT DISTINCT

    pseudonymous_users.value['pseudo_user_id']::VARCHAR                          AS pseudo_user_id,
    audiences.value['id']::NUMBER                                                AS audience_id,
    audiences.value['name']::VARCHAR                                             AS audience_name,
    TO_TIMESTAMP(audiences.value['membership_expiry_timestamp_micros']::VARCHAR) AS audience_membership_expiry_timestamp_micros,
    TO_TIMESTAMP(audiences.value['membership_start_timestamp_micros']::VARCHAR)  AS audience_membership_start_timestamp_micros,
    audiences.value['npa']::BOOLEAN                                              AS audience_npa,
    pseudonymous_users.value['date_part']::DATE                                  AS date_part

  FROM source AS pseudonymous_users,
    TABLE(FLATTEN(value['audiences'])) AS audiences

)

SELECT *
FROM flattened
