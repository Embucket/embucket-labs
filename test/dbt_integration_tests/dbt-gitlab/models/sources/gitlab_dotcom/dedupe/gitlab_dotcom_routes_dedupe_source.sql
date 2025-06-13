SELECT *
FROM {{ source('gitlab_dotcom', 'routes') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
