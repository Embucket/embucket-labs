#!/usr/bin/env python3
"""Debug script to see what the parser is actually matching."""

import re

def debug_parse_dbt_output(dbt_output):
    """Debug version of parse_dbt_output."""
    # Remove ANSI color codes
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    clean_output = ansi_escape.sub('', dbt_output)
    
    lines = clean_output.split('\n')
    
    print("=== DEBUGGING DBT PARSER ===")
    print(f"Total lines: {len(lines)}")
    
    # Look for lines that might match our patterns
    success_pattern = r'(\d+ of \d+) OK (created|loaded) (sql \w+ model|seed file) ([^\s]+) \[(SUCCESS|CREATE) (\d+) in ([\d.]+)s\]'
    error_pattern = r'(\d+ of \d+) ERROR (creating|loading) (sql \w+ model|seed file) ([^\s]+) \[ERROR in ([\d.]+)s\]'
    skip_pattern = r'(\d+ of \d+) SKIP relation ([^\s]+) [\.]+ \[SKIP\]'
    hook_success_pattern = r'(\d+ of \d+) OK hook: ([^\s]+) [\.]+ \[OK in ([\d.]+)s\]'
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Look for lines containing "OK" or "ERROR" or "SKIP"
        if any(keyword in line for keyword in ['OK', 'ERROR', 'SKIP']):
            print(f"\nLine {i}: {line}")
            
            # Test each pattern
            if re.match(success_pattern, line):
                print("  -> MATCHES SUCCESS PATTERN")
            elif re.match(error_pattern, line):
                print("  -> MATCHES ERROR PATTERN")
            elif re.match(skip_pattern, line):
                print("  -> MATCHES SKIP PATTERN")
            elif re.match(hook_success_pattern, line):
                print("  -> MATCHES HOOK PATTERN")
            else:
                print("  -> NO PATTERN MATCH")

if __name__ == "__main__":
    with open('dbt_output.log', 'r') as f:
        dbt_output = f.read()
    
    debug_parse_dbt_output(dbt_output)