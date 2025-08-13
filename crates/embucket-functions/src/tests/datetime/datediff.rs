use crate::test_query;

test_query!(
    years,
    "SELECT DATEDIFF('year', '2020-04-09 14:39:20'::TIMESTAMP, '2023-05-08 23:39:20'::TIMESTAMP) AS diff_years;",
    snapshot_path = "datediff"
);

test_query!(
    hours,
    "SELECT DATEDIFF('hour',
               '2023-05-08T23:39:20.123-07:00'::TIMESTAMP,
               DATEADD('year', 2, ('2023-05-08T23:39:20.123-07:00')::TIMESTAMP))
    AS diff_hours;",
    snapshot_path = "datediff"
);

test_query!(
    combined,
    "SELECT d,
       DATEDIFF('year', '2017-01-01'::DATE, d) as result_year,
       DATEDIFF('week', '2017-01-01'::DATE, d) as result_week,
       DATEDIFF('day', '2017-01-01'::DATE, d) as result_day,
       DATEDIFF('hour', '2017-01-01'::DATE, d) as result_hour,
       DATEDIFF('minute', '2017-01-01'::DATE, d) as result_minute,
       DATEDIFF('second', '2017-01-01'::DATE, d) as result_second
  FROM VALUES
       ('2016-12-30'::DATE),
       ('2016-12-31'::DATE),
       ('2017-01-01'::DATE),
       ('2017-01-02'::DATE),
       ('2017-01-03'::DATE),
       ('2017-01-04'::DATE),
       ('2017-01-05'::DATE),
       ('2017-12-30'::DATE),
       ('2017-12-31'::DATE)
  AS t(d);",
    snapshot_path = "datediff"
);
