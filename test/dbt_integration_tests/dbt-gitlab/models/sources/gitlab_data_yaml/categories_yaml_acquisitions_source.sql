WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'categories') }}
    ORDER BY uploaded_at DESC

), split_acquisition_info AS (

    SELECT 
      category.value['name']::VARCHAR  	    AS category_name,
	  category.value['stage']::VARCHAR 	    AS category_stage,
      acquisition.value::VARIANT            AS acquisition_object,
	  acquisition.key::VARCHAR         	    AS acquisition_key,
      acquisition_info.value                AS info_object,
	  acquisition_info.key                  AS info_key,
	  DATE_TRUNC('day', uploaded_at)::DATE  AS snapshot_date,
	  rank
	FROM source,
	LATERAL FLATTEN(INPUT => parse_json(null), OUTER => TRUE) category,
    LATERAL FLATTEN(INPUT => parse_json(null), OUTER => TRUE) acquisition,
    TABLE(FLATTEN(input => acquisition.value, RECURSIVE => TRUE))  acquisition_info
    WHERE acquisition.key ILIKE 'acquisition_%'
      AND acquisition_info.key in ('name', 'start_date', 'end_date')

), info_combined AS (

  -- Combine back the list of objects about each acquisition into a single object
  SELECT
    category_name,
    category_stage,
    rank,
    snapshot_date,
    acquisition_key,
    OBJECT_AGG(info_key, info_object) acquisition_info
  FROM split_acquisition_info
  {{ dbt_utils.group_by(n=5) }}
  
), info_parsed AS (

  -- Parse the object about each acquisition
  SELECT
    category_name,
    category_stage,
    rank,
    snapshot_date,
    acquisition_key,
    acquisition_info['name']::VARCHAR    AS acquisition_name,
    acquisition_info['start_date']::DATE AS acquisition_start_date,
    acquisition_info['end_date']::DATE   AS acquisition_end_date
  FROM info_combined

)

SELECT *
FROM info_parsed
