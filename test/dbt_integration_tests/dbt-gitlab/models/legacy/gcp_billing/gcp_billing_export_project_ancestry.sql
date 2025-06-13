{{ config(
    materialized= 'incremental',
    unique_key= 'project_folder_label_pk',
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh()
    )
}}

WITH source AS (

  SELECT 
    primary_key,
    uploaded_at,
    project_ancestors
  FROM {{ ref('summary_gcp_billing_source') }}
  {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }})

  {% endif %}

),

renamed AS (

  SELECT
    source.primary_key                                                                        AS source_primary_key,
    project_ancestors_flat.value['display_name']::VARCHAR                                     AS folder_name,
    SPLIT_PART(project_ancestors_flat.value['resource_name']::VARCHAR, '/', 2)                AS folder_id,
    ROW_NUMBER() OVER (PARTITION BY source.primary_key ORDER BY project_ancestors_flat.index) AS hierarchy_level,
    source.uploaded_at                                                                        AS uploaded_at,
    {{ dbt_utils.generate_surrogate_key([
            'source_primary_key',
            'folder_name',
            'folder_id'] ) }}                                                                                            AS project_folder_label_pk
  FROM source
  INNER JOIN LATERAL FLATTEN(input=> source.project_ancestors) project_ancestors_flat
)

SELECT *
FROM renamed
