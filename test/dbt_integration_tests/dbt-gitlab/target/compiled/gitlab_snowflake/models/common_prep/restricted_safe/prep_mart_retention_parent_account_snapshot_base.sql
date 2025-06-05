WITH base AS (

    SELECT *
    FROM "EMBUCKET".snapshots.mart_retention_parent_account_snapshot
    
)

SELECT *
FROM base