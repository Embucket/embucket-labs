use crate::test_query;

test_query!(
    type_of_all,
    "SELECT
        TYPEOF(TO_ARRAY('Example')) AS array1,
        TYPEOF(ARRAY_CONSTRUCT('Array-like', 'example')) AS array2,
        TYPEOF(TRUE) AS boolean1,
        TYPEOF('X') AS varchar1,
        TYPEOF('I am a real character') AS varchar2,
        TYPEOF(1.23::DECIMAL(6, 3)) AS decimal1,
        TYPEOF(3.21::DOUBLE) AS double1,
        TYPEOF(15) AS integer1,
        TYPEOF(TO_OBJECT(PARSE_JSON('{\"Tree\": \"Pine\"}'))) AS object1",
    snapshot_path = "type_of"
);
