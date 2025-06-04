WITH source AS (

  SELECT *
  FROM {{ source('linkedin_ads','campaign_history') }}

),

renamed AS (

  SELECT
	id::NUMBER AS campaign_id,
	last_modified_time::TIMESTAMP AS last_modified_time,
	type::VARCHAR AS type,
    objective_type::VARCHAR as objective_type,
	created_time::TIMESTAMP AS created_time,
	name::VARCHAR AS name,
	status::VARCHAR AS status,
	run_schedule_start::TIMESTAMP AS run_schedule_start,
	run_schedule_end::TIMESTAMP AS run_schedule_end,
	campaign_group_id::NUMBER AS campaign_group_id,
	account_id::NUMBER AS account_id,
	_fivetran_synced::TIMESTAMP AS _fivetran_synced

  FROM source
  
)

SELECT *
FROM renamed