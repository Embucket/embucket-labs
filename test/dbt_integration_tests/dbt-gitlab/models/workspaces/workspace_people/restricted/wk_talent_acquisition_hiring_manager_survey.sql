SELECT
  responded_at::DATE                  AS responded_at,
  overall_rating::INTEGER             AS overall_rating,
  sourced_by_ta,
  recruiter_communication::INTEGER    AS recruiter_communication,
  ta_customer_service_rating::INTEGER AS ta_customer_service_rating
FROM {{ ref('hiring_manager_survey') }}
