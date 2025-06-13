WITH
source AS (
  SELECT
    alertid::INT                    AS alertid,
    ack_time::INT                   AS ack_time,
    alert_name::VARCHAR             AS alert_name,
    alert_time::INT                 AS alert_time,
    assignee::VARCHAR               AS assignee,
    change_assignee::VARCHAR        AS change_assignee,
    close_alert::INT                AS close_alert,
    comments::VARCHAR               AS comments,
    convert_incident::BOOLEAN       AS convert_incident,
    TO_DATE(db_date, 'YY-MM-DD')    AS db_date,
    event_time::INT                 AS event_time,
    feature::VARCHAR                AS feature,
    label::VARCHAR                  AS label,
    mitre_tac::VARCHAR              AS mitre_tac,
    mitre_teq::VARCHAR              AS mitre_teq,
    sentiment::VARCHAR              AS sentiment,
    gcs_uploaded_at::DATE           AS gcs_uploaded_at,
    snowflake_uploaded_at::DATETIME AS snowflake_uploaded_at
  FROM
    {{ source('sirt_alertapp', 'sirt_alertapp_data') }}
),

dedupped AS (
  SELECT *
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY alertid ORDER BY gcs_uploaded_at DESC) = 1
)

SELECT *
FROM dedupped
