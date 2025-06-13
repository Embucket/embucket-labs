WITH prep_app_release AS (

    SELECT
        major_version || '.' || minor_version AS major_minor_version,
        major_version,
        minor_version,
        application,
        MIN(release_date) AS release_date
    FROM {{ ref('prep_app_release') }}
    WHERE dim_app_release_sk != {{ dbt_utils.generate_surrogate_key(['-1']) }} -- filter out missing member
    GROUP BY 1,2,3,4

), yaml_source AS (

    SELECT
        major_minor_version,
        'GitLab' AS application,
        major_version,
        minor_version,
        release_date,
        release_manager_americas,
        release_manager_emea
    FROM {{ ref('releases_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY major_minor_version ORDER BY snapshot_date DESC, rank DESC) = 1

), joined AS (

    SELECT
        COALESCE(prep_app_release.major_minor_version, yaml_source.major_minor_version)             AS major_minor_version,
        COALESCE(prep_app_release.application, yaml_source.application)                             AS application,
        COALESCE(prep_app_release.major_version, yaml_source.major_version)                         AS major_version,
        COALESCE(prep_app_release.minor_version, yaml_source.minor_version)                         AS minor_version,
        COALESCE(prep_app_release.release_date, yaml_source.release_date)                           AS release_date,
        IFNULL(yaml_source.release_manager_americas, 'Missing gitlab_release_manager_americas')     AS gitlab_release_manager_americas,
        IFNULL(yaml_source.release_manager_emea, 'Missing gitlab_release_manager_emea')             AS gitlab_release_manager_emea
    FROM prep_app_release
    FULL OUTER JOIN yaml_source
      ON prep_app_release.major_version = yaml_source.major_version
      AND prep_app_release.minor_version = yaml_source.minor_version

), add_keys AS (

    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['application', 'major_minor_version']) }}                       AS dim_app_release_major_minor_sk,

        -- Natural key
        CONCAT(application, '-', major_minor_version)                                               AS app_release_major_minor_id,


        major_version * 100 + minor_version                                                         AS major_minor_version_num,
        major_minor_version,
        application,
        major_version,
        minor_version,
        release_date,
        gitlab_release_manager_americas,
        gitlab_release_manager_emea
    FROM joined

), final AS (

    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY release_date)                      AS version_number,
        LEAD(release_date) OVER (ORDER BY release_date)                AS next_version_release_date
    FROM add_keys

    UNION ALL

    -- Add missing member information
    SELECT
    --surrogate_key
      {{ dbt_utils.generate_surrogate_key(['-1']) }}     AS dim_app_release_major_minor_sk,

      --natural key
      '-1'                                      AS app_release_major_minor_id,
    
     --attributes
     -1                                         AS major_minor_version_num,
     'Missing major_minor_version'              AS major_minor_version,
     'Missing application'                      AS application,
     -1                                         AS major_version,
     -1                                         AS minor_version,
     '9999-12-31'                               AS release_date,
    'Missing gitlab_release_manager_americas'   AS gitlab_release_manager_americas,
    'Missing gitlab_release_manager_emea'       AS gitlab_release_manager_emea,
    -1                                          AS version_number,
    '9999-12-31'                                AS next_version_release_date
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2023-04-04",
    updated_date="2024-03-07"
) }}
