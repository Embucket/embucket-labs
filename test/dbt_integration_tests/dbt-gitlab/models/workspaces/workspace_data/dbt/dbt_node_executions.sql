{{
  config(
    materialized='view',
    enabled=false
  )
}}

WITH model_fct AS (
  SELECT *
  FROM {{ ref('fct_dbt__model_executions') }}
),

model_dim AS (
  SELECT *
  FROM {{ ref('dim_dbt__models') }}
),

snapshot_fct AS (
  SELECT *
  FROM {{ ref('fct_dbt__snapshot_executions') }}
),

snapshot_dim AS (
  SELECT *
  FROM {{ ref('dim_dbt__snapshots') }}
),

seed_fct AS (
  SELECT *
  FROM {{ ref('fct_dbt__seed_executions') }}
),

seed_dim AS (
  SELECT *
  FROM {{ ref('dim_dbt__seeds') }}
),

test_fct AS (
  SELECT *
  FROM {{ ref('fct_dbt__test_executions') }}
),

test_dim AS (
  SELECT *
  FROM {{ ref('dim_dbt__tests') }}
),

models AS (
  SELECT
    model_fct.command_invocation_id,
    'model'                                      AS node_type,
    model_fct.node_id,
    model_fct.name                               AS node_name,
    REGEXP_SUBSTR(model_fct.thread_id, '[0-9]+') AS thread_id,
    model_fct.materialization,
    model_fct.was_full_refresh,
    model_fct.status                             AS node_status,
    model_dim.package_name,
    model_dim.path                               AS node_path,
    model_dim.meta                               AS node_meta,
    model_dim.tags                               AS node_tags,
    model_dim.depends_on_nodes,
    model_fct.run_started_at,
    model_fct.compile_started_at,
    model_fct.query_completed_at,
    model_fct.total_node_runtime
  FROM model_fct
  LEFT JOIN model_dim
    ON model_fct.command_invocation_id = model_dim.command_invocation_id
      AND model_fct.node_id = model_dim.node_id


),

snapshots AS (
  SELECT
    snapshot_fct.command_invocation_id,
    'snapshot'                                      AS node_type,
    snapshot_fct.node_id,
    snapshot_fct.name,
    REGEXP_SUBSTR(snapshot_fct.thread_id, '[0-9]+') AS thread_id,
    snapshot_fct.materialization,
    snapshot_fct.was_full_refresh,
    snapshot_fct.status,
    snapshot_dim.package_name,
    snapshot_dim.path,
    snapshot_dim.meta,
    ARRAY_CONSTRUCT()                               AS tags,
    snapshot_dim.depends_on_nodes,
    snapshot_fct.run_started_at,
    snapshot_fct.compile_started_at,
    snapshot_fct.query_completed_at,
    snapshot_fct.total_node_runtime
  FROM snapshot_fct
  LEFT JOIN snapshot_dim
    ON snapshot_fct.command_invocation_id = snapshot_dim.command_invocation_id
      AND snapshot_fct.node_id = snapshot_dim.node_id

),

seeds AS (
  SELECT
    seed_fct.command_invocation_id,
    'seed'                                      AS node_type,
    seed_fct.node_id,
    seed_fct.name,
    REGEXP_SUBSTR(seed_fct.thread_id, '[0-9]+') AS thread_id,
    seed_fct.materialization,
    seed_fct.was_full_refresh,
    seed_fct.status,
    seed_dim.package_name,
    seed_dim.path,
    seed_dim.meta,
    ARRAY_CONSTRUCT()                           AS tags,
    ARRAY_CONSTRUCT()                           AS depends_on_nodes,
    seed_fct.run_started_at,
    seed_fct.compile_started_at,
    seed_fct.query_completed_at,
    seed_fct.total_node_runtime
  FROM seed_fct
  LEFT JOIN seed_dim
    ON seed_fct.command_invocation_id = seed_dim.command_invocation_id
      AND seed_fct.node_id = seed_dim.node_id


),

tests AS (
  SELECT
    test_fct.command_invocation_id,
    'test'                                      AS node_type,
    test_fct.node_id,
    SPLIT_PART(test_fct.node_id, '.', 3)        AS node_name,
    REGEXP_SUBSTR(test_fct.thread_id, '[0-9]+') AS thread_id,
    'test'                                      AS materialization,
    test_fct.was_full_refresh,
    test_fct.status,
    test_dim.package_name,
    test_dim.test_path,
    OBJECT_CONSTRUCT()                          AS meta,
    test_dim.tags,
    test_dim.depends_on_nodes,
    test_fct.run_started_at,
    test_fct.compile_started_at,
    test_fct.query_completed_at,
    test_fct.total_node_runtime
  FROM test_fct
  LEFT JOIN test_dim
    ON test_fct.command_invocation_id = test_dim.command_invocation_id
      AND test_fct.node_id = test_dim.node_id


),

unions AS (
  SELECT *
  FROM models

  UNION ALL

  SELECT *
  FROM seeds

  UNION ALL

  SELECT *
  FROM snapshots

  UNION ALL

  SELECT *
  FROM tests
)

SELECT * FROM unions
