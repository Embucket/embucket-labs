# embucket-functions

Defines and registers various functions to extend the capabilities of the DataFusion query engine used within Embucket.

## Purpose

This crate enhances the DataFusion query engine with Embucket-specific or commonly needed SQL functions that are not part of the standard DataFusion distribution.

## For Contributors

📖 **[Functions Implementation Guide](docs/function_implementation_guide)** - Complete guide for implementing functions in Embucket

🔧 **[Function Template](src/scalar_template.rs)** - Ready-to-use template for creating new scalar functions

## Categories

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