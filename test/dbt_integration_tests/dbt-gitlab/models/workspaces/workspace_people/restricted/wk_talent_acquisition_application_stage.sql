WITH greenhouse_jobs_stages_source AS (
  SELECT *
  FROM {{ ref('greenhouse_jobs_stages_source') }}
),

greenhouse_stages_source AS (
  SELECT *
  FROM {{ ref('greenhouse_stages_source') }}
),stg
AS (
	SELECT j_stg.*
		,stg.stage_name AS job_stage_name_modified
	FROM greenhouse_jobs_stages_source j_stg
	LEFT JOIN greenhouse_stages_source stg ON j_stg.job_stage_id = stg.stage_id
	)
	,intermediate
AS (
	SELECT job_id
		,job_stage_order
		,job_stage_name_modified
		,job_stage_id
		,job_stage_milestone
	FROM stg
	
	UNION ALL
	
	SELECT job_id
		,100 AS job_stage_order
		,'Rejected' AS job_stage_name
		,100 AS job_stage_id_false
		,'Rejected' AS job_stage_milestone
	FROM stg qualify row_number() OVER (
			PARTITION BY job_id ORDER BY job_stage_name_modified DESC
			) = 1
	
	UNION ALL
	
	SELECT job_id
		,1000 AS job_stage_order
		,'Hired' AS job_stage_name_modified
		,1000 AS job_stage_id
		,'Hired' AS job_stage_milestone
	FROM stg qualify row_number() OVER (
			PARTITION BY job_id ORDER BY job_stage_name_modified DESC
			) = 1
	
	UNION ALL
	
	SELECT job_id
		,10000 AS job_stage_order
		,'Started' AS job_stage_name_modified
		,10000 AS job_stage_id
		,'Started' AS job_stage_milestone
	FROM greenhouse_jobs_stages_source qualify row_number() OVER (
			PARTITION BY job_id ORDER BY job_stage_name_modified DESC
			) = 1
	
	UNION ALL
	
	SELECT job_id
		,- 1 AS job_stage_order
		,'Application Submitted' AS job_stage_name_modified
		,- 1 AS job_stage_id
		,'Application' AS job_stage_milestone
	FROM greenhouse_jobs_stages_source
	WHERE job_stage_order = 1
	ORDER BY 1
		,2
	)
SELECT *
FROM intermediate
ORDER BY 1
	,2
