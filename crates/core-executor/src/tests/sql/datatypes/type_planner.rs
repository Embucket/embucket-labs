use crate::test_query;

test_query!(
    characters,
    "SELECT
        cast('data' as character varying(16777216)) as t1,
        cast('data' as character(16777216)) as t2,
        cast('data' as char(16777216)) as t3",
    snapshot_path = "type_planner"
);
