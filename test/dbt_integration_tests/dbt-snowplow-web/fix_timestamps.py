#!/usr/bin/env python3
"""
Script to fix timestamp format in events.csv file.
The issue is that timestamps like '2022-08-20 0:00:01' need to be 
changed to '2022-08-20 00:00:01' for Arrow's timestamp parser.
"""

import re
import sys
from pathlib import Path

def fix_timestamps_in_csv(file_path):
    """Fix timestamp format in CSV file by padding single-digit hours with zeros."""
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match timestamps like '2022-08-20 0:00:01' and replace with '2022-08-20 00:00:01'
    # This regex looks for a date pattern followed by a space, single digit, colon, two digits, colon, two digits
    pattern = r'(\d{4}-\d{2}-\d{2} )(\d:)(\d{2}:\d{2})'
    replacement = r'\g<1>0\g<2>\g<3>'
    
    # Apply the replacement
    fixed_content = re.sub(pattern, replacement, content)
    
    # Check if any changes were made
    if fixed_content == content:
        print("No timestamp format issues found.")
        return False
    
    # Write the fixed content back to the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print(f"Fixed timestamp format in {file_path}")
    return True

def main():
    csv_file = Path("events.csv")
    
    if not csv_file.exists():
        print(f"Error: {csv_file} not found")
        sys.exit(1)
    
    print(f"Fixing timestamps in {csv_file}...")
    fixed = fix_timestamps_in_csv(csv_file)
    
    if fixed:
        print("Timestamp format has been fixed successfully!")
    else:
        print("No changes were needed.")

if __name__ == "__main__":
    main() 