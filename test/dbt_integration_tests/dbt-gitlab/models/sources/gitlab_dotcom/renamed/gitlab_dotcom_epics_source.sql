WITH all_epics AS (

  SELECT
    id::NUMBER                          AS epic_id,
    group_id::NUMBER                    AS group_id,
    author_id::NUMBER                   AS author_id,
    assignee_id::NUMBER                 AS assignee_id,
    iid::NUMBER                         AS epic_internal_id,
    updated_by_id::NUMBER               AS updated_by_id,
    last_edited_by_id::NUMBER           AS last_edited_by_id,
    lock_version::NUMBER                AS lock_version,
    start_date::DATE                    AS epic_start_date,
    end_date::DATE                      AS epic_end_date,
    last_edited_at::TIMESTAMP           AS epic_last_edited_at,
    created_at::TIMESTAMP               AS created_at,
    updated_at::TIMESTAMP               AS updated_at,
    closed_at::TIMESTAMP                AS closed_at,
    state_id::NUMBER                    AS state_id,
    parent_id::NUMBER                   AS parent_id,
    relative_position::NUMBER           AS relative_position,
    start_date_sourcing_epic_id::NUMBER AS start_date_sourcing_epic_id,
    confidential::BOOLEAN               AS is_confidential,
    {{ map_state_id('state_id') }}                                 AS state,
    LENGTH(title)::NUMBER               AS epic_title_length,
    LENGTH(description)::NUMBER         AS epic_description_length
  FROM {{ ref('gitlab_dotcom_epics_dedupe_source') }}

),

interal_epics AS (

  SELECT
    id::NUMBER           AS internal_epic_id,
    iid::NUMBER          AS internal_epic_iid,
    title::VARCHAR       AS internal_epic_title,
    description::VARCHAR AS internal_epic_description -- has this bneen nullifed?
  FROM {{ ref('gitlab_dotcom_epics_internal_only_dedupe_source') }}

),

joined AS (

  SELECT
    all_epics.epic_id                       AS epic_id,
    all_epics.group_id                      AS group_id,
    all_epics.author_id                     AS author_id,
    all_epics.assignee_id                   AS assignee_id,
    all_epics.updated_by_id                 AS updated_by_id,
    all_epics.last_edited_by_id             AS last_edited_by_id,
    all_epics.lock_version                  AS lock_version,
    all_epics.epic_start_date               AS epic_start_date,
    all_epics.epic_end_date                 AS epic_end_date,
    all_epics.epic_last_edited_at           AS epic_last_edited_at,
    all_epics.created_at                    AS created_at,
    all_epics.updated_at                    AS updated_at,
    interal_epics.internal_epic_title       AS epic_title,
    interal_epics.internal_epic_description AS epic_description,
    all_epics.closed_at                     AS closed_at,
    all_epics.state_id                      AS state_id,
    all_epics.parent_id                     AS parent_id,
    all_epics.relative_position             AS relative_position,
    all_epics.start_date_sourcing_epic_id   AS start_date_sourcing_epic_id,
    all_epics.is_confidential               AS is_confidential,
    all_epics.state                         AS state,
    all_epics.epic_title_length             AS epic_title_length,
    all_epics.epic_description_length       AS epic_description_length
  FROM all_epics
  LEFT JOIN interal_epics
    ON all_epics.epic_id = interal_epics.internal_epic_id

)

SELECT *
FROM joined
