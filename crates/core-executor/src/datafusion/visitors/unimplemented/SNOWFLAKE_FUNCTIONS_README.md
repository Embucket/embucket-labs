# Unimplemented Snowflake Functions Registry

This document describes the new organization structure for unimplemented Snowflake functions in the codebase.

## Overview

The Snowflake functions are now organized into separate hash maps by category. This provides better organization and easier navigation for tracking unimplemented functions.

## Structure

### Main Components

- **`FunctionInfo`**: Contains metadata about each function including name, description, documentation URL, and issue URL (for tracking implementation progress)
- **`SnowflakeFunctions`**: Main container with separate hash maps for each category

### Categories

The functions are organized into the following categories:

1. **Numeric Functions** (`numeric_functions`)
   - Mathematical operations like ABS, COS, SIN, LOG, etc.
   
2. **String & Binary Functions** (`string_binary_functions`)
   - String manipulation like CONCAT, SUBSTR, TRIM, etc.
   - Regular expression functions (subcategory: "regex")
   
3. **Date & Time Functions** (`date_time_functions`)
   - Date/time operations like DATEADD, EXTRACT, etc.
   
4. **Semi-structured Functions** (`semi_structured_functions`)
   - Array functions (subcategory: "array")
   - Object functions (subcategory: "object") 
   - JSON functions (subcategory: "json")
   
5. **Aggregate Functions** (`aggregate_functions`)
   - SUM, AVG, COUNT, etc.
   
6. **Window Functions** (`window_functions`)
   - ROW_NUMBER, RANK, etc.
   
7. **Conditional Functions** (`conditional_functions`)
   - CASE, COALESCE, IFF, etc.
   
8. **Conversion Functions** (`conversion_functions`)
   - CAST, TO_CHAR, TO_DATE, etc.
   
9. **Context Functions** (`context_functions`)
   - CURRENT_USER, CURRENT_TIMESTAMP, etc.
   
10. **System Functions** (`system_functions`)
    - System-level operations
    
11. **Additional Categories**
    - Geospatial, Hash, Encryption, File, Notification, Generation, Vector, etc.

## Usage

### Basic Usage

```rust
use crate::datafusion::visitors::unimplemented_functions_list::*;

let functions = SnowflakeFunctions::new();

// Check if a function is unimplemented
if functions.is_unimplemented("ARRAY_CONSTRUCT") {
    // Handle unimplemented function
}

// Get function information
if let Some(func_info) = functions.numeric_functions.get("ABS") {
    println!("Function: {}", func_info.name);
    println!("Description: {}", func_info.description);
    if let Some(url) = func_info.documentation_url {
        println!("Documentation: {}", url);
    }
}
```

### Adding Issue URLs

When you create GitHub issues for specific functions, you can update the `issue_url` field:

```rust
let mut functions = SnowflakeFunctions::new();
if let Some(func_info) = functions.numeric_functions.get_mut("ABS") {
    func_info.issue_url = Some("https://github.com/your-repo/issues/123");
}
```

### Iterating by Category

```rust
// List all functions in a specific category
for (name, info) in &functions.numeric_functions {
    println!("Numeric function: {} - {}", name, info.description);
}

// List functions with subcategories
for (name, info) in &functions.string_binary_functions {
    if info.subcategory == Some("regex") {
        println!("Regex function: {}", name);
    }
}
```

## Extension

To add more functions:

1. Add the function to the appropriate category in the `populate_*` methods
2. Include documentation URL from Snowflake docs
3. Add subcategory if applicable
4. Update issue URL when GitHub issue is created

## Benefits

- **Organized Structure**: Functions grouped by logical categories
- **Documentation Links**: Direct links to Snowflake documentation
- **Issue Tracking**: Placeholder for GitHub issue URLs
- **Easy Navigation**: Separate hash maps for different function types
- **Extensible**: Easy to add new functions and categories
- **Backward Compatible**: Old `UNIMPLEMENTED_FUNCTIONS` constant maintained

## Files

- `unimplemented_functions_list.rs` - Main structure and core categories
- `snowflake_functions_extended.rs` - Extension methods for additional categories
- This README explains the organization and usage 