-- infra label to pl mapping

SELECT * FROM EMBUCKET.seed_engineering.gcp_billing_infra_pl_mapping
UNPIVOT(allocation FOR type IN (free, internal, paid))