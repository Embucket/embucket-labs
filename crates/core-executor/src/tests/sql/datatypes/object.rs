use crate::test_query;

test_query!(
    object_null_cast,
    "SELECT CAST(null as OBJECT) as test;",
    snapshot_path = "object"
);

test_query!(
    binary_to_binary_formats,
    "SELECT
      {
        'state': 'CA',
        'city': 'San Mateo',
        'street': '450 Concar Drive',
        'zip_code': 94402
      }::OBJECT(
        state VARCHAR,
        city VARCHAR,
        street VARCHAR,
        zip_code NUMBER
      );",
    snapshot_path = "object"
);
