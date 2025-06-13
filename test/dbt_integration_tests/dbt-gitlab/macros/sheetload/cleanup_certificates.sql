{%- macro cleanup_certificates(certificate_name) -%}

clean_score AS (

	SELECT
		completed_date,
		submitter_name,
		TRIM(SPLIT_PART(score, '/', 1))::NUMBER AS correct_responses,
		TRIM(SPLIT_PART(score, '/', 2))::NUMBER AS total_responses,
    CASE WHEN LOWER(submitter_email) LIKE '%@gitlab.com%'
      THEN True
      ELSE False
      END                                       AS is_team_member,
    CASE WHEN LOWER(submitter_email) LIKE '%@gitlab.com%'
      THEN TRIM(LOWER(submitter_email))
      ELSE md5(submitter_email) END             AS submitter_email,
    {{certificate_name}}                        AS certificate_name,
		last_updated_at
	FROM source

)

SELECT *
FROM clean_score

{%- endmacro -%}
