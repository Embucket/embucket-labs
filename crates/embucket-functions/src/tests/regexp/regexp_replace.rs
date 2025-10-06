use crate::test_query;

test_query!(
    regexp_replace_basic,
    "SELECT REGEXP_REPLACE('nevermore1, nevermore2, nevermore3.', 'nevermore', 'moreover')",
    snapshot_path = "regexp_replace"
);
test_query!(
    regexp_replace_basic_utf8_view,
    "SELECT REGEXP_REPLACE(CAST('nevermore1, nevermore2, nevermore3.' as VARCHAR), 'nevermore', 'moreover')",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_remove,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times',
                      '( ){1,}',
                      '')",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_occurrence,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times',
                      'times',
                      'days',
                      1,
                      2)",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_first_occurrence_not_forward,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times. times',
                      'times',
                      'days',
                      1,
                      1)",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_second_occurrence_not_forward,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times. times',
                      'times',
                      'days',
                      1,
                      2)",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_third_occurrence_not_forward,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times. times',
                      'times',
                      'days',
                      1,
                      3)",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_first_occurrence_not_forward_with_position,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times. times',
                      'times',
                      'days',
                      30,
                      1) AS result;",
    snapshot_path = "regexp_replace"
);

test_query!(
    regexp_replace_second_occurrence_not_forward_with_position,
    "SELECT REGEXP_REPLACE('It was the best of times, it was the worst of times. times',
                      'times',
                      'days',
                      30,
                      2) AS result;",
    snapshot_path = "regexp_replace"
);
