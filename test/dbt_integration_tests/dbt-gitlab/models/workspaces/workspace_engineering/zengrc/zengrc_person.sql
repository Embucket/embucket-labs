{{ simple_cte([
    ('audits','zengrc_audit_source'),
    ('assessments','zengrc_assessment_source'),
    ('requests','zengrc_request_source')
])}}

, audit_managers AS (

    SELECT DISTINCT
      audut_manager.value['id']::NUMBER    AS person_id,
      audut_manager.value['name']::VARCHAR AS person_name,
      audut_manager.value['type']::VARCHAR AS zengrc_type
    FROM audits
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(audits.audit_managers)) audut_manager

), assessors AS (

    SELECT DISTINCT
      assessors.value['id']::NUMBER    AS person_id,
      assessors.value['name']::VARCHAR AS person_name,
      assessors.value['type']::VARCHAR AS zengrc_type
    FROM assessments
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(assessments.assessors)) assessors

), assignees AS (

    SELECT DISTINCT
      assignees.value['id']::NUMBER    AS person_id,
      assignees.value['name']::VARCHAR AS person_name,
      assignees.value['type']::VARCHAR AS zengrc_type
    FROM requests
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(requests.assignees)) assignees

), requestors AS (

    SELECT DISTINCT
      requestors.value['id']::NUMBER    AS person_id,
      requestors.value['name']::VARCHAR AS person_name,
      requestors.value['type']::VARCHAR AS zengrc_type
    FROM requests
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(requests.requestors)) requestors

), unioned AS (

    SELECT *
    FROM audit_managers

    UNION

    SELECT *
    FROM assessors

    UNION

    SELECT *
    FROM assignees

    UNION

    SELECT *
    FROM requestors
)

SELECT
  *
FROM unioned