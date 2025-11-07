use crate::test_query;

test_query!(
    file,
    "SELECT volume_name, volume_type FROM sqlite.meta.volumes",
    setup_queries = ["CREATE EXTERNAL VOLUME file STORAGE_LOCATIONS = (\
        (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '/home/'))"],
    snapshot_path = "volume"
);

test_query!(
    memory,
    "SELECT volume_name, volume_type FROM sqlite.meta.volumes",
    setup_queries = ["CREATE EXTERNAL VOLUME mem STORAGE_LOCATIONS = (\
        (NAME = 'mem_vol' STORAGE_PROVIDER = 'MEMORY'))"],
    snapshot_path = "volume"
);

test_query!(
    memory_if_not_exists,
    "SELECT volume_name, volume_type FROM sqlite.meta.volumes",
    setup_queries = [
        "CREATE EXTERNAL VOLUME mem STORAGE_LOCATIONS = ((NAME = 'mem_vol' STORAGE_PROVIDER = 'MEMORY'))",
        "CREATE EXTERNAL VOLUME IF NOT EXISTS mem STORAGE_LOCATIONS = ((NAME = 'mem_vol' STORAGE_PROVIDER = 'MEMORY'))",
    ],
    snapshot_path = "volume"
);

test_query!(
    s3,
    "SELECT volume_name, volume_type FROM sqlite.meta.volumes",
    setup_queries = ["CREATE EXTERNAL VOLUME s3 STORAGE_LOCATIONS = ((
            NAME = 's3-volume' STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 'bucket_name'
            STORAGE_ENDPOINT = 'https://s3.us-east-2.amazonaws.com'
            CREDENTIALS=(AWS_KEY_ID='1a2b3c...' AWS_SECRET_KEY='4x5y6z...' REGION='us-east-2')
        ))"],
    snapshot_path = "volume"
);

test_query!(
    s3tables,
    "SELECT volume_name, volume_type FROM sqlite.meta.volumes",
    setup_queries = [
        "CREATE EXTERNAL VOLUME s3 STORAGE_LOCATIONS = ((
            NAME = 's3-volume' STORAGE_PROVIDER = 'S3TABLES'
            STORAGE_ENDPOINT = 'https://s3.us-east-2.amazonaws.com'
            STORAGE_AWS_ACCESS_POINT_ARN = 'arn:aws:s3tables:us-east-1:111122223333:bucket/my-embucket'
            CREDENTIALS=(AWS_KEY_ID='1a2b3c...' AWS_SECRET_KEY='4x5y6z...')
        ))"
    ],
    snapshot_path = "volume"
);
