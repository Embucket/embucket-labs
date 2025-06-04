{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'dim_installation_id', not_null_test_cols = []) }}",
    "tags":["mnpi_exception"]
}) }}

{{ simple_cte([

  ('prep_host', 'prep_host'),
  ('prep_ping_instance', 'prep_ping_instance'),
  ('prep_ping_instance_flattened', 'prep_ping_instance_flattened'),
  ('prep_latest_seat_link_installation', 'prep_latest_seat_link_installation'),
  ('internal_installations', 'internal_gitlab_installations')
  ]

)}},

installation_agg AS (

  SELECT
    dim_installation_id,
    MIN(TRY_TO_TIMESTAMP(TRIM(metric_value::VARCHAR,'UTC'))) AS installation_creation_date
  FROM prep_ping_instance_flattened
  WHERE ping_created_at > '2023-03-15' --filtering out records that came before GitLab v15.10, when metric was released. Filter in place for full refresh runs.
    AND metrics_path IN (
      'installation_creation_date_approximation',
      'installation_creation_date'
    )
    AND metric_value != 0 -- 0, when cast to timestamp, returns 1970-01-01

    -- filtering out these two installations per this discussion: 
    -- https://gitlab.com/gitlab-data/analytics/-/merge_requests/8416#note_1473684265
    AND dim_installation_id NOT IN (
      'cc89cba853f6caf1a100259b1048704f',
      '5f03898242847ecbaff5b5f8973d5910',
      '41e744c635b09914b02635a9542c7773'
    )
  {{ dbt_utils.group_by(n = 1) }}

),

most_recent_ping AS (

-- Accounting for installation that change from one delivery/deployment type to another.
-- Typically, this is a Self-Managed installation that converts to Dedicated.
-- Additionally, an installation can change its hostname, causing different is_staging values at different times.

  SELECT
    dim_installation_id,
    ping_delivery_type,
    ping_deployment_type,
    is_staging
  FROM prep_ping_instance
  QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_installation_id ORDER BY ping_created_at DESC) = 1

),

service_ping_installations AS (

  SELECT DISTINCT
    -- Primary Key
    prep_ping_instance.dim_installation_id,

    -- Natural Keys 
    prep_ping_instance.dim_instance_id,
    prep_ping_instance.dim_host_id,

    -- Dimensional contexts
    prep_host.host_name,
    installation_agg.installation_creation_date,
    most_recent_ping.ping_delivery_type   AS product_delivery_type,
    most_recent_ping.ping_deployment_type AS product_deployment_type,
    prep_ping_instance.is_internal,
    most_recent_ping.is_staging

  FROM prep_ping_instance
  INNER JOIN prep_host 
    ON prep_ping_instance.dim_host_id = prep_host.dim_host_id
  LEFT JOIN installation_agg 
    ON prep_ping_instance.dim_installation_id = installation_agg.dim_installation_id
  LEFT JOIN most_recent_ping
    ON most_recent_ping.dim_installation_id = prep_ping_instance.dim_installation_id

),

seat_link_installations AS (

    SELECT DISTINCT
    -- Primary Key
    prep_latest_seat_link_installation.dim_installation_id,

    -- Natural Keys
    prep_latest_seat_link_installation.dim_instance_id,
    prep_latest_seat_link_installation.dim_host_id,

    -- Dimensional contexts
    prep_host.host_name,
    NULL                                                                                          AS installation_creation_date,
    IFF(
      prep_latest_seat_link_installation.product_delivery_type = '(Unknown Delivery Type)',
      NULL,
      prep_latest_seat_link_installation.product_delivery_type)                                   AS product_delivery_type,
    IFF(
      prep_latest_seat_link_installation.product_deployment_type = '(Unknown Deployment Type)',
      NULL,
      prep_latest_seat_link_installation.product_deployment_type
      )                                                                                           AS product_deployment_type,
    IFF(internal_installations.dim_installation_id IS NOT NULL, TRUE, FALSE)                      AS is_internal,
    CASE
      WHEN prep_latest_seat_link_installation.host_name ILIKE 'staging.%'
        OR prep_latest_seat_link_installation.host_name = 'dr.gitlab.com' THEN TRUE
      ELSE FALSE 
    END                                                                                           AS is_staging
  FROM prep_latest_seat_link_installation
  INNER JOIN prep_host
    ON prep_latest_seat_link_installation.dim_host_id = prep_host.dim_host_id
  LEFT JOIN internal_installations
    ON internal_installations.dim_installation_id = prep_latest_seat_link_installation.dim_installation_id
  WHERE prep_latest_seat_link_installation.dim_instance_id IS NOT NULL

)

  SELECT DISTINCT
    -- Primary Key
    COALESCE(service_ping_installations.dim_installation_id, seat_link_installations.dim_installation_id)                 AS dim_installation_id,

    -- Natural Keys 
    COALESCE(service_ping_installations.dim_instance_id, seat_link_installations.dim_instance_id)                         AS dim_instance_id,
    COALESCE(service_ping_installations.dim_host_id, seat_link_installations.dim_host_id)                                 AS dim_host_id,

    -- Dimensional contexts  
    COALESCE(service_ping_installations.host_name, seat_link_installations.host_name)                                     AS host_name,
    COALESCE(service_ping_installations.installation_creation_date, seat_link_installations.installation_creation_date)   AS installation_creation_date,
    COALESCE(service_ping_installations.product_delivery_type, seat_link_installations.product_delivery_type)             AS product_delivery_type,
    COALESCE(service_ping_installations.product_deployment_type, seat_link_installations.product_deployment_type)         AS product_deployment_type,
    COALESCE(service_ping_installations.is_internal, seat_link_installations.is_internal)                                 AS is_internal,
    COALESCE(service_ping_installations.is_staging, seat_link_installations.is_staging)                                   AS is_staging
    
  FROM service_ping_installations
  FULL OUTER JOIN seat_link_installations
    ON service_ping_installations.dim_installation_id = seat_link_installations.dim_installation_id
