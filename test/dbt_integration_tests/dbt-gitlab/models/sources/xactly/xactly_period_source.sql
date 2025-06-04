WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_period') }}

),

renamed AS (

  SELECT

    period_id::FLOAT AS period_id,
    version::FLOAT AS version,
    name::VARCHAR AS name,
    start_date::VARCHAR AS start_date,
    end_date::VARCHAR AS end_date,
    is_active::VARCHAR AS is_active,
    is_open::VARCHAR AS is_open,
    parent_period_id::FLOAT AS parent_period_id,
    period_type_id_fk::FLOAT AS period_type_id_fk,
    calendar_id::FLOAT AS calendar_id,
    order_number::FLOAT AS order_number,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    is_published::VARCHAR AS is_published,
    is_visible::VARCHAR AS is_visible,
    is_etl_excluded::VARCHAR AS is_etl_excluded,
    is_hidden::VARCHAR AS is_hidden,
    is_calc_period::VARCHAR AS is_calc_period

  FROM source

)

SELECT *
FROM renamed
