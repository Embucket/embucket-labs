use crate::test_query;

test_query!(
    status_message_use,
    "USE DATABASE embucket",
    snapshot_path = "status_message"
);

test_query!(
    status_message_set,
    "SET test = 2",
    snapshot_path = "status_message"
);
