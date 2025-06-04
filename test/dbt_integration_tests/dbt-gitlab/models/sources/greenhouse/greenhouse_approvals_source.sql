WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'approvals') }}

), renamed as (

	SELECT

            --keys
            offer_id::NUMBER            AS offer_id,
            application_id::NUMBER      AS application_id,
            job_id::NUMBER              AS job_id,
            candidate_id::NUMBER        AS candidate_id,
            approver_id::NUMBER         AS approver_id,
            group_id::NUMBER            AS group_id,

            --info
            approval_type::varchar      AS approval_type,
            status::varchar             AS approval_status,
            version::NUMBER                AS approval_version,
            final_version::NUMBER          AS approval_version_final,
            group_order::NUMBER            AS group_order,
            group_quorum::NUMBER           AS group_quorum,
            assigned::timestamp         AS approval_assigned_at,
            completed::timestamp        AS approval_completed_at,
            created_at::timestamp       AS approval_created_at,
            updated_at::timestamp       AS approval_updated_at

	FROM source

)

SELECT *
FROM renamed
