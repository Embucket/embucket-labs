use crate::test_query;

test_query!(
    parse_ipv4,
    "SELECT PARSE_IP('192.168.242.188/24', 'INET') AS ipv4",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_ipv6,
    "SELECT PARSE_IP('fe80::20c:29ff:fe2c:429/64', 'INET') AS ipv6",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_permissive_error,
    "SELECT PARSE_IP('not_an_ip', 'INET', 1) AS err",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_permissive_error_int_nonzero,
    "SELECT PARSE_IP('not_an_ip', 'INET', 5) AS err",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_permissive_error_float_nonzero,
    "SELECT PARSE_IP('not_an_ip', 'INET', 0.5) AS err",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_non_permissive_error_zero,
    "SELECT PARSE_IP('not_an_ip', 'INET', 0) AS err",
    snapshot_path = "parse_ip"
);

test_query!(
    parse_non_permissive_error_float_rounds_to_zero,
    "SELECT PARSE_IP('not_an_ip', 'INET', 0.4) AS err",
    snapshot_path = "parse_ip"
);
