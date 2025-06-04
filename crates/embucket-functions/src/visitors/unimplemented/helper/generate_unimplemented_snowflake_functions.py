#!/usr/bin/env python3

import csv
import re
from collections import defaultdict
from pathlib import Path

def normalize_category_name(category: str) -> str:
    """Convert category name to a valid Rust const identifier"""
    # Remove special characters and normalize
    normalized = re.sub(r'[^\w\s]', '', category)
    # Replace spaces and convert to UPPER_SNAKE_CASE
    normalized = re.sub(r'\s+', '_', normalized.strip())
    return normalized.upper()

def escape_rust_string(s: str) -> str:
    """Escape special characters for Rust string literals"""
    if not s:
        return '""'
    # Escape backslashes and quotes
    escaped = s.replace('\\', '\\\\').replace('"', '\\"')
    return f'"{escaped}"'

def clean_function_name(name: str) -> str:
    """Clean function name by removing specifications in parentheses"""
    # Remove anything in parentheses (like "(system data metric function)")
    cleaned = re.sub(r'\s*\([^)]*\)\s*', '', name.strip())
    return cleaned.strip()

def is_deprecated_function(name: str, description: str = "") -> bool:
    """Check if a function is deprecated or obsoleted"""
    # Check for deprecated/obsoleted markers in name or description
    deprecated_markers = ['deprecate', 'obsolete']
    
    combined_text = f"{name} {description}".lower()
    return any(marker in combined_text for marker in deprecated_markers)

def should_exclude_function(name: str) -> bool:
    """Check if a function should be excluded entirely"""
    cleaned_name = clean_function_name(name)
    
    if '[NOT]' in cleaned_name or '[ NOT ]' in cleaned_name:
        return True
    
    return False

def expand_wildcard_pattern(pattern: str) -> list:
    """Expand wildcard patterns (ending with *)"""
    # Define wildcard expansion patterns
    wildcard_expansions = {
        'YEAR*': [
            'YEAR',
            'YEAROFWEEK', 
            'YEAROFWEEKISO'
        ],
        'DAY*': [
            'DAY',
            'DAYOFMONTH',
            'DAYOFWEEK',
            'DAYOFWEEKISO',
            'DAYOFYEAR'
        ],
        'WEEK*': [
            'WEEK',
            'WEEKOFYEAR',
            'WEEKISO'
        ],
        'TO_TIMESTAMP_*': [
            'TRY_TO_TIMESTAMP',
            'TRY_TO_TIMESTAMP_LTZ', 
            'TRY_TO_TIMESTAMP_NTZ',
            'TRY_TO_TIMESTAMP_TZ'
        ],
        'AS_TIMESTAMP_*': [
            'AS_TIMESTAMP_LTZ',
            'AS_TIMESTAMP_NTZ', 
            'AS_TIMESTAMP_TZ'
        ],
        'IS_TIMESTAMP_*': [
            'IS_TIMESTAMP_LTZ',
            'IS_TIMESTAMP_NTZ',
            'IS_TIMESTAMP_TZ'
        ]
    }
    
    return wildcard_expansions.get(pattern, [])

def expand_special_patterns(pattern: str) -> list:
    """Expand special patterns like SYSTEM$LOG_<level>"""
    special_expansions = {
        'SYSTEM$LOG_<level>': [
            'SYSTEM$LOG_TRACE',
            'SYSTEM$LOG_DEBUG',
            'SYSTEM$LOG_INFO', 
            'SYSTEM$LOG_WARN',
            'SYSTEM$LOG_ERROR',
            'SYSTEM$LOG_FATAL'
        ]
    }
    
    return special_expansions.get(pattern, [])

def expand_function_patterns(name: str) -> list:
    """Expand wildcard patterns into specific function variants"""
    cleaned_name = clean_function_name(name)
    
    # Check if function should be excluded entirely
    if should_exclude_function(name):
        return []  # Return empty list to exclude
       
    # First handle comma or slash-separated function names
    if ',' in cleaned_name or '/' in cleaned_name:
        # Split by comma or slash and clean each part
        variants = []
        # Use regex to split by comma or slash while preserving whitespace handling
        separators = re.split(r'[,/]', cleaned_name)
        
        for variant in separators:
            variant = variant.strip()
            # Skip empty variants and common operators/symbols
            if variant and variant not in [':', '::', '||', 'NOT', '']:
                # Handle special patterns that need expansion
                if variant.endswith('*'):
                    # This is a wildcard pattern, expand it
                    expanded = expand_wildcard_pattern(variant)
                    if expanded:
                        variants.extend(expanded)
                    continue
                special_expanded = expand_special_patterns(variant)
                if special_expanded:
                    variants.extend(special_expanded)
                    continue
                
                variants.append(variant)
        
        if variants: 
            return variants
        else:
            # If all variants were empty/invalid, fall through to pattern matching
            cleaned_name = re.split(r'[,/]', cleaned_name)[0].strip()  # Use first part
    
    # Handle single wildcard patterns
    if cleaned_name.endswith('*'):
        expanded = expand_wildcard_pattern(cleaned_name)
        if expanded:
            return expanded
     
    return [cleaned_name]

def load_implemented_functions(implemented_file: str) -> set:
    """Load the list of already implemented functions from CSV file"""
    implemented_functions = set()
    
    if not Path(implemented_file).exists():
        print(f"Warning: Implemented functions file '{implemented_file}' not found!")
        return implemented_functions
    
    try:
        with open(implemented_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Always skip the first row (SQL query header)
            next(reader, None)
            
            # Read the function names
            for row in reader:
                if row and row[0].strip():
                    function_name = row[0].strip().upper()
                    implemented_functions.add(function_name)
        
        print(f"Loaded {len(implemented_functions)} implemented functions from {implemented_file}")
        return implemented_functions
        
    except Exception as e:
        print(f"Error reading implemented functions file: {e}")
        return set()

def generate_rust_file(functions_by_category: dict, output_file: str):
    """Generate the Rust file with const arrays"""
    
    content = []
    content.append("// This file is auto-generated by generate_snowflake_functions.py")
    content.append("// Do not edit manually!")
    content.append("")
    content.append("use super::FunctionInfo;")
    content.append("")
    
    # Generate const arrays for each category
    for category, functions in sorted(functions_by_category.items()):
        const_name = f"{normalize_category_name(category)}_FUNCTIONS"
        content.append(f"pub const {const_name}: &[(&str, FunctionInfo)] = &[")
        
        for func in sorted(functions, key=lambda x: x['name']):
            name = func['name']
            description = func['description']
            docs_url = func.get('docs_url', '')
            subcategory = func.get('subcategory', '')
            
            content.append(f"    ({escape_rust_string(name)}, FunctionInfo::new(")
            content.append(f"        {escape_rust_string(name)},")
            content.append(f"        {escape_rust_string(description)}")
            content.append("    )")
            
            if docs_url:
                content.append(f"    .with_docs({escape_rust_string(docs_url)})")
            
            if subcategory:
                content.append(f"    .with_subcategory({escape_rust_string(subcategory)})")
            
            content.append("    ),")
        
        content.append("];")
        content.append("")
     
       
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))

def main():
    csv_file = "BuiltInFunctions_Snowflake.csv"
    implemented_file = "implemented_functions.csv"
    output_file = "../generated_snowflake_functions.rs"
    
    # Check if CSV file exists
    if not Path(csv_file).exists():
        print(f"Error: CSV file '{csv_file}' not found!")
        return
    
    # Load implemented functions to exclude
    implemented_functions = load_implemented_functions(implemented_file)
    
    functions_by_category = defaultdict(list)
    total_functions_processed = 0
    excluded_functions_count = 0
    expanded_functions_count = 0
    deprecated_functions_count = 0
    pattern_excluded_count = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        # Get headers
        headers = next(reader)
        print(f"CSV headers: {headers}")
        
        # Find column indices based on actual CSV structure:
        # A, <description>, <snowflake_docs_section>, <mapping_info>, Proposed category, Subcategory(If applied), PureURL
        try:
            name_col = 0  # Column A (function name)
            description_col = 1  # Second column (description)
            # Skip column 2 (Snowflake docs section) - we don't use this for categorization
            # Skip column 3 (mapping info)
            category_col = headers.index('Proposed category')  # Column 4 - this is what we want for categorization
            subcategory_col = headers.index('Subcategory(If applied)')  # Column 5
            docs_col = headers.index('PureURL')  # Column 6
            
            print(f"Using columns: name={name_col}, description={description_col}, category={category_col}, subcategory={subcategory_col}, docs={docs_col}")
            
        except ValueError as e:
            print(f"Error finding columns: {e}")
            print(f"Available headers: {headers}")
            return
        
        # Process each row
        for row_num, row in enumerate(reader, start=2):  # Start at 2 since we skip header
            # Skip empty rows or rows that are too short
            if not row or len(row) <= max(name_col, description_col, category_col, docs_col):
                continue
            
            raw_name = row[name_col].strip() if len(row) > name_col else ''
            description = row[description_col].strip() if len(row) > description_col else ''
            category = row[category_col].strip() if len(row) > category_col else ''
            subcategory = row[subcategory_col].strip() if len(row) > subcategory_col else ''
            docs_url = row[docs_col].strip() if len(row) > docs_col else ''
            
            if not raw_name or not category:
                continue
            
            # Skip deprecated/obsoleted functions
            if is_deprecated_function(raw_name, description):
                deprecated_functions_count += 1
                print(f"Skipping deprecated function: {raw_name}")
                continue
            
            total_functions_processed += 1
            
            # Clean and expand function names
            expanded_names = expand_function_patterns(raw_name)
            
            # Check if function was excluded by pattern
            if not expanded_names:
                pattern_excluded_count += 1
                print(f"Excluding function by pattern: {raw_name}")
                continue
            
            # If we expanded the function name, track it
            if len(expanded_names) > 1:
                expanded_functions_count += 1
                print(f"Expanded '{raw_name}' to: {', '.join(expanded_names)}")
            elif clean_function_name(raw_name) != raw_name:
                print(f"Cleaned '{raw_name}' to '{expanded_names[0]}'")
            
            # Process each expanded function name
            for name in expanded_names:
                # Check if this function is already implemented
                function_name_upper = name.upper()
                if function_name_upper in implemented_functions:
                    excluded_functions_count += 1
                    print(f"Excluding already implemented function: {name}")
                    continue
                
                # Use the proposed category directly (values like "numeric", "aggregate", etc.)
                primary_category = category.strip()
                
                function_info = {
                    'name': name,
                    'description': description,
                    'subcategory': subcategory if subcategory else None,
                    'docs_url': docs_url if docs_url else None
                }
                
                functions_by_category[primary_category].append(function_info)
                
                # Debug: print first few entries to verify
                if row_num <= 5:
                    print(f"Row {row_num}: {name} -> category: '{primary_category}', subcategory: '{subcategory}'")
    
    if not functions_by_category:
        print("Error: No unimplemented functions found in CSV file!")
        return
    
    print(f"\nProcessed {total_functions_processed} total functions")
    print(f"Skipped {deprecated_functions_count} deprecated functions")
    print(f"Excluded {pattern_excluded_count} functions by pattern")
    print(f"Expanded {expanded_functions_count} wildcard patterns")
    print(f"Excluded {excluded_functions_count} already implemented functions")
    print(f"Found {sum(len(funcs) for funcs in functions_by_category.values())} unimplemented functions in {len(functions_by_category)} categories:")
    for category, funcs in sorted(functions_by_category.items()):
        print(f"  {category}: {len(funcs)} functions")
    
    # Generate the Rust file
    generate_rust_file(functions_by_category, output_file)
    print(f"\nGenerated Rust file: {output_file}")

if __name__ == "__main__":
    main() 