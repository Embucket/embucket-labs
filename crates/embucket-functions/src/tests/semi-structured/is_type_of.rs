use crate::test_query;

test_query!(
    type_of_all,
    "SELECT
        IS_DECIMAL(1.23::DECIMAL(6, 3)) AS decimal,
        IS_DECIMAL(3.21::DOUBLE) AS dec_double,
        IS_DECIMAL(15) AS decimal_int,
        IS_BOOLEAN(TRUE) AS boolean,
        IS_INTEGER('123') as int,
        IS_INTEGER(NULL) as int_null,
        IS_DOUBLE('123') as double_str,
        IS_DOUBLE('3.14') as double,
        IS_DOUBLE(3.14) as double_num",
    snapshot_path = "is_type_of"
);
