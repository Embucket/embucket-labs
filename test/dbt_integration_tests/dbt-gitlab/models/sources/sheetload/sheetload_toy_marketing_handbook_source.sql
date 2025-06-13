WITH source AS (

  SELECT
    age::int AS age,
    job,
    marital,
    education,
    "DEFAULT" AS is_default,
    balance::int AS balance,
    housing,
    loan,
    contact,
    day::int AS day,
    month,
    duration::int AS duration,
    num_contacts_during_campaign::int AS num_contacts_during_campaign,
    prev_campaign_passed_days::int AS prev_campaign_passed_days,
    prev_contacts::int AS prev_contacts,
    prev_outcome,
    outcome
  FROM {{ source('sheetload', 'toy_marketing_handbook') }}

)

SELECT *
FROM source
