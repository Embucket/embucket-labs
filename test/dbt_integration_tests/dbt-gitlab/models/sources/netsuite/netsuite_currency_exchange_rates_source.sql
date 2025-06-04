WITH source AS (

  SELECT *
  FROM {{ source('netsuite', 'currency_exchange_rates') }}

), renamed AS (

  SELECT

    --Primary Key
    currency_rate_id::FLOAT AS currency_rate_id,

    --Foreign keys
    anchor_currency_id::FLOAT AS anchor_currency_id,
    base_currency_id::FLOAT AS base_currency_id,
    currency_id::FLOAT AS currency_id,
    currency_rate_provider_id::VARCHAR AS currency_rate_provider_id,
    currency_rate_type_id::FLOAT AS currency_rate_type_id,
    entity_id::VARCHAR AS entity_id,

    --Information
    exchange_rate::FLOAT AS exchange_rate,
    is_anchor_only::VARCHAR AS is_anchor_only,

    --Metadata
    date_effective::DATE AS date_effective,
    date_last_modified::DATE AS date_last_modified

  FROM source

)

SELECT *
FROM renamed