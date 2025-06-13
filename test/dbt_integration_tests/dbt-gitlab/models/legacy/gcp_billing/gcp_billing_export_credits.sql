{{ config(
    materialized="incremental",
    unique_key="credit_pk",
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh()
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('summary_gcp_billing_source')}}
    {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

    {% endif %}

), renamed as (

    SELECT
        source.primary_key                      AS source_primary_key,
        credits_flat.value['name']::VARCHAR     AS credit_description,
        credits_flat.value['amount']::FLOAT * source.occurrence_multiplier AS credit_amount,
        source.uploaded_at                      AS uploaded_at,
        {{ dbt_utils.generate_surrogate_key([
            'source_primary_key',
            'credit_description',
            'credits_flat.value'] ) }}          AS credit_pk
    FROM source,
    LATERAL FLATTEN(input => credits) credits_flat

)

SELECT *
FROM renamed
