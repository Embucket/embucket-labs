-- infra label to pl mapping

SELECT * FROM {{ ref ('gcp_billing_haproxy_pl_mapping') }}
UNPIVOT(allocation FOR type IN (free, internal, paid))
