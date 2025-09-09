use crate::test_query;

test_query!(
    like_type_analyzer_basic,
    "SELECT column1 FROM VALUES (910), (256) WHERE '%http' LIKE column1",
    snapshot_path = "like_type_analyzer"
);

test_query!(
    like_type_analyzer_reversed,
    "SELECT column1 FROM VALUES (910), (256) WHERE column1 LIKE '%http'",
    snapshot_path = "like_type_analyzer"
);

test_query!(
    like_type_analyzer_both,
    "SELECT column1 FROM VALUES (910), (256) WHERE column1 LIKE column1",
    snapshot_path = "like_type_analyzer"
);

test_query!(
    like_type_analyzer_none,
    "SELECT column1 FROM VALUES (910), (256) WHERE '%http' LIKE '%http'",
    snapshot_path = "like_type_analyzer"
);
