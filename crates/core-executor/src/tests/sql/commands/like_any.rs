use crate::test_query;

test_query!(
    like_any_even,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%','T%e')
            ORDER BY column1",
    snapshot_path = "like_any"
);

test_query!(
    like_any_odd,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%','T%e', '%Tim%')
            ORDER BY column1",
    snapshot_path = "like_any"
);

test_query!(
    like_any_odd_wrong,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%','T%e', '%Tim%', '%YES%')
            ORDER BY column1",
    snapshot_path = "like_any"
);

test_query!(
    like_any_even_wrong,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%', '%YES%')
            ORDER BY column1",
    snapshot_path = "like_any"
);

test_query!(
    like_any_one,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%')
            ORDER BY column1",
    snapshot_path = "like_any"
);

test_query!(
    like_any_one_wrong,
    "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%YES%')
            ORDER BY column1",
    snapshot_path = "like_any"
);
