WITH source AS (

  SELECT *
  FROM {{ ref('hackerone_source') }}

),

base AS (

  SELECT
    id,
    state,
    created_at,
    bounties_array,
    SUM(f.value:attributes:amount::NUMBER)                                                                        AS total_amount,
    REPLACE(bounties_array[0].relationships.report.data.attributes.issue_tracker_reference_url::VARIANT, '"', '') AS issue_url,
    uploaded_at
  FROM source,
    LATERAL FLATTEN(source.bounties_array, outer => TRUE) AS f
  GROUP BY
    ALL

)

SELECT *
FROM base
