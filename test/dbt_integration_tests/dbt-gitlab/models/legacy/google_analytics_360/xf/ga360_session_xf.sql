WITH sessions AS (

	SELECT *
	FROM {{ ref('ga360_session') }}

)

SELECT *
FROM sessions
