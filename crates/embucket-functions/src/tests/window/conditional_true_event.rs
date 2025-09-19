use crate::test_query;

test_query!(
    basic,
    "SELECT id, flag,
            CONDITIONAL_TRUE_EVENT(flag) OVER (ORDER BY id) AS evt
     FROM (VALUES (1, TRUE), (2, TRUE), (3, FALSE), (4, TRUE), (5, TRUE), (6, FALSE), (7, NULL), (8, TRUE)) AS t(id, flag);",
    snapshot_path = "conditional_true_event"
);

test_query!(
    start_false,
    "SELECT id, flag,
            CONDITIONAL_TRUE_EVENT(flag) OVER (ORDER BY id) AS evt
     FROM (VALUES (1, FALSE), (2, TRUE), (3, FALSE), (4, TRUE)) AS t(id, flag);",
    snapshot_path = "conditional_true_event"
);

test_query!(
    partition_o_gt_2,
    "SELECT p, o,
            CONDITIONAL_TRUE_EVENT(o > 2) OVER (PARTITION BY p ORDER BY o) AS evt
     FROM (
         VALUES
         (100,1,1,70,'seventy'),
         (100,2,2,30,'thirty'),
         (100,3,3,40,'fourty'),
         (100,4,NULL,90,'ninety'),
         (100,5,5,50,'fifty'),
         (100,6,6,30,'thirty'),
         (200,7,7,40,'fourty'),
         (200,8,NULL,NULL,'n_u_l_l'),
         (200,9,NULL,NULL,'n_u_l_l'),
         (200,10,10,20,'twenty'),
         (200,11,NULL,90,'ninety'),
         (300,12,12,30,'thirty'),
         (400,13,NULL,20,'twenty')
     ) AS tbl(p, o, i, r, s)
     ORDER BY p, o;",
    snapshot_path = "conditional_true_event"
);

test_query!(
    partition_o_gt_2_all_number,
    "SELECT province, o_col, 
      CONDITIONAL_TRUE_EVENT(o_col) 
        OVER (PARTITION BY province ORDER BY o_col) 
          AS true_event
    FROM (
         VALUES
         ('Alberta',    0, 10),
         ('Alberta',    0, 10),
         ('Alberta',   13, 10),
         ('Alberta',   13, 11),
         ('Alberta',   14, 11),
         ('Alberta',   15, 12),
         ('Alberta', NULL, NULL),
         ('Manitoba',    30, 30)
    ) AS tbl(province, o_col, i_col)
    ORDER BY province, o_col;",
    snapshot_path = "conditional_true_event"
);
