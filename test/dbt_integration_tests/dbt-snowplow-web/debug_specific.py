#!/usr/bin/env python3
"""Debug specific lines that should match."""

import re

# Test the specific lines we saw in the debug output
test_lines = [
    "13:16:07  3 of 3 OK loaded seed file public_snowplow_manifest.snowplow_web_dim_rfc_5646_language_mapping  [CREATE 232 in 2.71s]",
    "13:16:21  2 of 29 OK created sql incremental model public_snowplow_manifest.snowplow_web_incremental_manifest  [SUCCESS 0 in 2.96s]",
    "13:16:33  6 of 29 ERROR creating sql table model public_scratch.snowplow_web_base_events_this_run  [ERROR in 1.82s]",
    "13:16:33  7 of 29 SKIP relation public_scratch.snowplow_web_consent_events_this_run ...... [SKIP]",
    "13:16:04  1 of 1 OK hook: snowplow_web.on-run-start.0 .................................... [OK in 0.01s]"
]

# Current patterns
success_pattern = r'(\d+ of \d+) OK (created|loaded) (sql \w+ model|seed file) ([^\s]+) \[(SUCCESS|CREATE) (\d+) in ([\d.]+)s\]'
error_pattern = r'(\d+ of \d+) ERROR (creating|loading) (sql \w+ model|seed file) ([^\s]+) \[ERROR in ([\d.]+)s\]'
skip_pattern = r'(\d+ of \d+) SKIP relation ([^\s]+) [\.]+ \[SKIP\]'
hook_success_pattern = r'(\d+ of \d+) OK hook: ([^\s]+) [\.]+ \[OK in ([\d.]+)s\]'

print("=== TESTING PATTERNS ===")
for i, line in enumerate(test_lines):
    print(f"\nLine {i+1}: {line}")
    
    # Test success pattern
    match = re.match(success_pattern, line)
    if match:
        print(f"  SUCCESS MATCH: {match.groups()}")
    else:
        print("  No success match")
    
    # Test error pattern
    match = re.match(error_pattern, line)
    if match:
        print(f"  ERROR MATCH: {match.groups()}")
    else:
        print("  No error match")
    
    # Test skip pattern
    match = re.match(skip_pattern, line)
    if match:
        print(f"  SKIP MATCH: {match.groups()}")
    else:
        print("  No skip match")
    
    # Test hook pattern
    match = re.match(hook_success_pattern, line)
    if match:
        print(f"  HOOK MATCH: {match.groups()}")
    else:
        print("  No hook match")