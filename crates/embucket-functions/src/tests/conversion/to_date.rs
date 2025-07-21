use crate::test_query;

test_query!(
    to_date_basic,
    "SELECT TO_DATE('2024-05-10') as to_date, DATE('2024-05-10') as date",
    snapshot_path = "to_date"
);

test_query!(
    to_date_from_timestamp,
    "SELECT TO_DATE(column1) FROM VALUES ('2024-10-02T04:00:00.000Z'::TIMESTAMP)",
    snapshot_path = "to_date"
);

test_query!(
    to_date_point_format,
    "SELECT TO_DATE('2024.05.10', 'YYYY.MM.DD') as to_date, DATE('2024.05.10', 'YYYY.MM.DD') as date",
    snapshot_path = "to_date"
);

test_query!(
    to_date_auto_format,
    "SELECT TO_DATE('2024-05-10', 'AUTO') as to_date, DATE('2024-05-10', 'AUTO') as date",
    snapshot_path = "to_date"
);

test_query!(
    to_date_slash_format,
    "SELECT TO_DATE('05/10/2024', 'MM/DD/YYYY') as to_date, DATE('05/10/2024', 'MM/DD/YYYY') as date",
    snapshot_path = "to_date"
);

test_query!(
    to_date_string_as_integer,
    "SELECT column1 as description, column2 as value, TO_DATE(column2) \
    FROM VALUES
        ('Seconds', '31536000'),
        ('Milliseconds', '31536000000'),
        ('Microseconds', '31536000000000'),
        ('Nanoseconds', '31536000000000000')",
    snapshot_path = "to_date"
);

test_query!(
    try_to_date,
    "SELECT TRY_TO_DATE('2024-05-10') AS valid_date,
        TRY_TO_DATE('Invalid') AS invalid_date",
    snapshot_path = "to_date"
);
