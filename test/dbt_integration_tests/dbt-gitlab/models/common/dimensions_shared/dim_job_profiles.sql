{{ config({
    "alias": "dim_job_profiles"
}) }}

WITH final AS (

  SELECT *
  FROM {{ ref('blended_job_profiles_source') }}

)

SELECT
  --surrogate key
  {{ dbt_utils.generate_surrogate_key(['job_workday_id']) }} AS dim_job_profile_sk,

  --natural_key
  job_workday_id,

  --Job Profile attributes
  job_code,
  job_profile,
  management_level,
  job_level,
  job_family,
  is_job_profile_active,
  --Time attributes
  valid_from,
  valid_to
FROM final
