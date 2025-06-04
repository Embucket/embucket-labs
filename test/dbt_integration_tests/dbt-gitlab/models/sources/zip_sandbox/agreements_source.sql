WITH source AS (
  SELECT *
  FROM {{ source('zip_sandbox','agreements') }}
),

intermediate AS (
  SELECT
    data.value,
    source.uploaded_at
  FROM
    source
  INNER JOIN LATERAL FLATTEN(input => source.jsontext['data']) AS data
),

parsed AS (
  SELECT
    value['agreement_number']::NUMBER                   AS agreement_number,
    value['agreement_owner']::VARIANT                   AS agreement_owner,
    value['agreement_status']::VARCHAR                  AS agreement_status,
    value['amount']::VARCHAR                            AS amount,
    value['attachments']::VARIANT                       AS attachments,
    value['attributes']::VARIANT                        AS attributes,
    value['auto_renews']::BOOLEAN                       AS does_auto_renews,
    value['categories']::VARIANT                        AS categories,
    value['child_requests']::VARIANT                    AS child_requests,
    value['currency']::VARCHAR                          AS currency,
    value['department']::VARCHAR                        AS department,
    value['display_renewal_status']::VARCHAR            AS display_renewal_status,
    value['end_date']::VARCHAR                          AS end_date,
    value['execution_date']::VARCHAR                    AS execution_date,
    value['id']::VARCHAR                                AS id,
    value['name']::VARCHAR                              AS name,
    value['opt_out_date']::VARCHAR                      AS opt_out_date,
    value['original_request']::VARIANT                  AS original_request,
    value['purchase_orders']::VARIANT                   AS purchase_orders,
    value['renewal_alert_date']::VARCHAR                AS renewal_alert_date,
    value['renewal_date']::VARCHAR                      AS renewal_date,
    value['renewal_stakeholder_queues']::VARIANT        AS renewal_stakeholder_queues,
    value['renewal_stakeholders']::VARIANT              AS renewal_stakeholders,
    value['renewal_status']::VARCHAR                    AS renewal_status,
    value['renewal_workflow']::VARIANT                  AS renewal_workflow,
    value['requests']::VARIANT                          AS requests,
    value['start_date']::VARCHAR                        AS start_date,
    value['subcategories']::VARIANT                     AS subcategories,
    value['subsidiaries']::VARIANT                      AS subsidiaries,
    value['type']::VARCHAR                              AS type,
    value['vendor']::VARIANT                            AS vendor,
    uploaded_at
  FROM intermediate
)

SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1
