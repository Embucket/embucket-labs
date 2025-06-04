{{ simple_cte([
    ('audits','zengrc_audit_source'),
    ('issues','zengrc_issue_source'),
    ('requests','zengrc_request_source')
])}}

, audit_programs AS (

    SELECT
      audits.program_id,
      audits.program_title,
      audits.program_type AS zengrc_object_type
    FROM audits
    WHERE audits.program_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY audits.program_id ORDER BY audit_uploaded_at DESC ) = 1

), issue_programs AS (

    SELECT
      mapped_programs.value['id']::NUMBER     AS program_id,
      mapped_programs.value['title']::VARCHAR AS program_title,
      mapped_programs.value['type']::VARCHAR  AS zengrc_type
    FROM issues
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(issues.mapped_programs)) mapped_programs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY program_id ORDER BY issue_updated_at DESC ) = 1

), requests_programs AS (

    SELECT
      mapped_programs.value['id']::NUMBER     AS program_id,
      mapped_programs.value['title']::VARCHAR AS program_title,
      mapped_programs.value['type']::VARCHAR  AS zengrc_type
    FROM requests
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(requests.mapped_programs)) mapped_programs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY program_id ORDER BY request_updated_at DESC ) = 1

), unioned AS (

    SELECT *
    FROM audit_programs

    UNION 

    SELECT *
    FROM issue_programs

    UNION 

    SELECT *
    FROM requests_programs

)

SELECT *
FROM unioned

