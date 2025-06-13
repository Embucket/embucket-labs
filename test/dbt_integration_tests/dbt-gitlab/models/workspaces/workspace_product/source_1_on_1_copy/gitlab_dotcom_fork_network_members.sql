WITH source AS (
	SELECT *
	FROM {{ ref('gitlab_dotcom_fork_network_members_source') }}
)
SELECT *
FROM source
