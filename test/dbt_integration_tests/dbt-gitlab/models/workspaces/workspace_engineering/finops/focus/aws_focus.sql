{% set source_tables = ['aws_billing_dedicated_dev_3675_source',
  'aws_billing_dedicated_legacy_0475_source',
  'aws_billing_gitlab_marketplace_5127_source',
  'aws_billing_itorg_3027_source',
  'aws_billing_legacy_gitlab_0347_source',
  'aws_billing_services_org_6953_source'] %}

{{ union_aws_source(source_tables) }}
