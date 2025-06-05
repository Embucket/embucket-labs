

SELECT *
FROM EMBUCKET.tap_zendesk.ticket_audits

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1