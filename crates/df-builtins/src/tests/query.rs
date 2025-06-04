use crate::test_query;

// Session context
test_query!(
    session_objects,
    "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
    snapshot_path = "session"
);
test_query!(
    session_current_schemas,
    "SELECT CURRENT_SCHEMAS()",
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
test_query!(
    session_current_session,
    "SELECT CURRENT_SESSION()",
    snapshot_path = "session"
);
test_query!(
    session_last_query_id,
    "SELECT LAST_QUERY_ID(), LAST_QUERY_ID(-1), LAST_QUERY_ID(2)",
    snapshot_path = "session"
);
test_query!(
    session_current_ip_address,
    "SELECT CURRENT_IP_ADDRESS()",
    snapshot_path = "session"
);
