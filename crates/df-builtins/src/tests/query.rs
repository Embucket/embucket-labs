use crate::test_query;

// Session context
test_query!(
    session_objects,
    "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
    snapshot_path = "session"
);
test_query!(
    session_general,
    "SELECT CURRENT_VERSION(), CURRENT_CLIENT()",
    snapshot_path = "session"
);
test_query!(
    session,
    "SELECT CURRENT_ROLE_TYPE(), CURRENT_ROLE()",
    snapshot_path = "session"
);
