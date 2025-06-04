WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'stage_snapshots') }}

), renamed as (

	SELECT
            --keys
            stage_id::NUMBER    AS stage_snapshot_id,
            job_id::NUMBER      AS job_id,

            --info
            date::date          AS stage_snapshot_date,
            active_count::NUMBER   AS stage_snapshot_active_count
	FROM source

)

SELECT *
FROM renamed
