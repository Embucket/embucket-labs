use crate::test_query;

test_query!(
    non_zero,
    "SELECT zeroifnull(3.14),zeroifnull(1),zeroifnull(3.14::decimal(20,10)) as d",
    snapshot_path = "zeroifnull"
);

test_query!(
    zero,
    "SELECT zeroifnull(null),zeroifnull(0.0),zeroifnull(0),zeroifnull(0.0::decimal(20,10)) as d",
    snapshot_path = "zeroifnull"
);
