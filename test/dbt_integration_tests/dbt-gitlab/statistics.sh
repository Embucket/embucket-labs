#!/bin/bash

# Clear the output file
> assets/top_errors.txt

# Write DBT Run Summary
grep "Done. PASS=" assets/run.log | tail -n 1 | awk -F'PASS=| WARN=| ERROR=| SKIP=| TOTAL=' '{print "# DBT Run Summary\nPASS: "$2"\nWARN: "$3"\nERROR: "$4"\nSKIP: "$5"\nTOTAL: "$6"\n"}' >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write errors grouped by high-level categories
echo "# Errors grouped by high-level categories" >> assets/top_errors.txt

# Extract all error messages and categorize them by major type
grep -A1 "Database Error\|Compilation Error" assets/run.log | grep -v "^--" | grep -v "Database Error\|Compilation Error" | awk '{
    error_msg = $0
    # Remove leading spaces and numeric error codes if present
    gsub(/^[[:space:]]*[0-9]+: /, "", error_msg)
    gsub(/^[[:space:]]+/, "", error_msg)  # Remove leading spaces
    
    if (error_msg ~ /^000200:/) {
        error_msg = substr(error_msg, 8)  # Remove "000200: " prefix
        if (error_msg ~ /^Optimizer rule/) print "Optimizer rule"
        else if (error_msg ~ /^Error during planning/) print "Error during planning"
        else if (error_msg ~ /^SQL compilation error/) print "SQL compilation error"
        else if (error_msg ~ /^SQL error/) print "SQL error"
        else if (error_msg ~ /^External error/) print "External error"
        else if (error_msg ~ /^Schema error/) print "Schema error"
        else if (error_msg ~ /^Arrow error/) print "Arrow error"
        else if (error_msg ~ /^Threaded Job error/) print "Threaded Job error"
        else if (error_msg ~ /^Invalid datatype/) print "Invalid datatype"
        else if (error_msg ~ /^This feature is not implemented/) print "Feature not implemented"
        else if (error_msg ~ /^type_coercion/) print "Type coercion error"
        else print "Other 000200 error"
    } else {
        # Handle specific error patterns by major category - grouped more broadly
        if (error_msg ~ /^expected string or bytes-like object/) print "Compilation error"
        else if (error_msg ~ /^Internal error: Function .* failed to match/) print "Error during planning"
        else if (error_msg ~ /^Function .* expects .* but received/) print "Error during planning"
        else if (error_msg ~ /^Internal error: Expect TypeSignatureClass/) print "Error during planning"
        else if (error_msg ~ /^Window has mismatch/) print "Error during planning"
        else if (error_msg ~ /^Internal error/) print "Error during planning"
        else if (error_msg ~ /^Inserting query must have the same schema/) print "Error during planning"
        else if (error_msg ~ /^column .* not found/) print "SQL compilation error"
        else if (error_msg ~ /^table .* not found/) print "SQL compilation error"
        else if (error_msg ~ /^No function matches the given name/) print "SQL compilation error"
        else if (error_msg ~ /^Cannot infer common argument type/) print "SQL compilation error"
        else if (error_msg ~ /^SQL compilation error: unsupported feature/) print "SQL compilation error"
        else if (error_msg ~ /^SQL compilation error/) print "SQL compilation error"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: \[EXTERNAL\] TABLE/) print "SQL error"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: literal string/) print "SQL error"
        else if (error_msg ~ /^SQL error: ParserError/) print "SQL error"
        else if (error_msg ~ /^SQL error/) print "SQL error"
        else if (error_msg ~ /^Error during planning/) print "Error during planning"
        else if (error_msg ~ /^External error/) print "External error"
        else if (error_msg ~ /^Schema error/) print "Schema error"
        else if (error_msg ~ /^This feature is not implemented/) print "Feature not implemented"
        else if (error_msg ~ /^type_coercion/) print "Type coercion error"
        else print "Other error"
    }
}' | sort | uniq -c | sort -nr >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write errors grouped by type (comprehensive analysis)
echo "# Errors grouped by type (comprehensive analysis)" >> assets/top_errors.txt

# Extract all error messages and categorize them in detail
grep -A1 "Database Error\|Compilation Error" assets/run.log | grep -v "^--" | grep -v "Database Error\|Compilation Error" | awk '{
    error_msg = $0
    # Remove leading spaces and numeric error codes if present
    gsub(/^[[:space:]]*[0-9]+: /, "", error_msg)
    gsub(/^[[:space:]]+/, "", error_msg)  # Remove leading spaces
    
    if (error_msg ~ /^000200:/) {
        error_msg = substr(error_msg, 8)  # Remove "000200: " prefix
        if (error_msg ~ /^Optimizer rule .optimize_projections. failed/) print "Optimizer rule: optimize_projections"
        else if (error_msg ~ /^Optimizer rule .common_sub_expression_eliminate. failed/) print "Optimizer rule: common_sub_expression_eliminate"
        else if (error_msg ~ /^Optimizer rule .eliminate_cross_join. failed/) print "Optimizer rule: eliminate_cross_join"
        else if (error_msg ~ /^Optimizer rule/) print "Optimizer rule: other"
        else if (error_msg ~ /^Error during planning: Inserting query must have the same schema/) print "Error during planning: schema mismatch"
        else if (error_msg ~ /^Error during planning: Internal error/) print "Error during planning: internal error"
        else if (error_msg ~ /^Error during planning: Function .* failed to match/) print "Error during planning: function signature"
        else if (error_msg ~ /^Error during planning: For function/) print "Error during planning: function type error"
        else if (error_msg ~ /^Error during planning: Unexpected argument/) print "Error during planning: argument error"
        else if (error_msg ~ /^Error during planning/) print "Error during planning: other"
        else if (error_msg ~ /^SQL compilation error: table .* not found/) print "SQL compilation error: table not found"
        else if (error_msg ~ /^SQL compilation error: column .* not found/) print "SQL compilation error: column not found"
        else if (error_msg ~ /^SQL compilation error: error line/) print "SQL compilation error: syntax error"
        else if (error_msg ~ /^SQL compilation error: unsupported feature/) print "SQL compilation error: unsupported feature"
        else if (error_msg ~ /^SQL compilation error/) print "SQL compilation error: other"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: literal string/) print "SQL error: ParserError - literal string"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: \[EXTERNAL\] TABLE/) print "SQL error: ParserError - CREATE statement"
        else if (error_msg ~ /^SQL error: ParserError/) print "SQL error: ParserError - other"
        else if (error_msg ~ /^SQL error/) print "SQL error: other"
        else if (error_msg ~ /^External error: Feature Arrow datatype/) print "External error: Arrow datatype"
        else if (error_msg ~ /^External error/) print "External error: other"
        else if (error_msg ~ /^Schema error: No field named/) print "Schema error: field not found"
        else if (error_msg ~ /^Schema error/) print "Schema error: other"
        else if (error_msg ~ /^Arrow error: Cast error/) print "Arrow error: cast error"
        else if (error_msg ~ /^Arrow error/) print "Arrow error: other"
        else if (error_msg ~ /^Threaded Job error/) print "Threaded Job error"
        else if (error_msg ~ /^Invalid datatype/) print "Invalid datatype"
        else if (error_msg ~ /^This feature is not implemented/) print "Feature not implemented"
        else if (error_msg ~ /^type_coercion/) print "Type coercion error"
        else if (error_msg ~ /^Arrow: Incompatible type/) print "Arrow error: incompatible type"
        else if (error_msg ~ /^Feature .* is not supported/) print "Feature not supported"
        else print "Other 000200 error"
    } else {
        # Handle specific error patterns in detail with better grouping
        if (error_msg ~ /^expected string or bytes-like object, got .Undefined./) print "Compilation error: Undefined object type mismatch"
        else if (error_msg ~ /^expected string or bytes-like object, got .None./) print "Compilation error: None object type mismatch"
        else if (error_msg ~ /^expected string or bytes-like object, got .Null./) print "Compilation error: Null object type mismatch"
        else if (error_msg ~ /^expected string or bytes-like object/) print "Compilation error: other type mismatch"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*md5.*Int32/) print "Error during planning: md5 function - Int32 type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*length.*Int32/) print "Error during planning: length function - Int32 type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*dateadd.*Float64/) print "Error during planning: dateadd function - Float64 type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*dateadd.*Int32/) print "Error during planning: dateadd function - Int32 type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*variant_element.*Boolean/) print "Error during planning: variant_element function - Boolean type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*dateadd/) print "Error during planning: dateadd function - other type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*md5/) print "Error during planning: md5 function - other type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*length/) print "Error during planning: length function - other type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature.*variant_element/) print "Error during planning: variant_element function - other type issue"
        else if (error_msg ~ /^Internal error: Function .* failed to match any signature/) print "Error during planning: function signature mismatch - other"
        else if (error_msg ~ /^Function .* expects .* but received/) print "Error during planning: function argument type mismatch"
        else if (error_msg ~ /^Internal error: Expect TypeSignatureClass/) print "Error during planning: internal type signature error"
        else if (error_msg ~ /^Window has mismatch/) print "Error during planning: window schema mismatch"
        else if (error_msg ~ /^Internal error/) print "Error during planning: internal error"
        else if (error_msg ~ /^Inserting query must have the same schema/) print "Error during planning: schema mismatch"
        else if (error_msg ~ /^This feature is not implemented.*ANY in LIKE/) print "Feature not implemented: ANY in LIKE expression"
        else if (error_msg ~ /^This feature is not implemented/) print "Feature not implemented: other"
        else if (error_msg ~ /^type_coercion/) print "Type coercion error"
        else if (error_msg ~ /^column .* not found/) print "SQL compilation error: column not found"
        else if (error_msg ~ /^table .* not found/) print "SQL compilation error: table not found"
        else if (error_msg ~ /^No function matches the given name/) print "SQL compilation error: function not found"
        else if (error_msg ~ /^Cannot infer common argument type/) print "SQL compilation error: type inference"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: \[EXTERNAL\] TABLE/) print "SQL error: ParserError - CREATE statement"
        else if (error_msg ~ /^SQL error: ParserError.*Expected: literal string/) print "SQL error: ParserError - literal string"
        else if (error_msg ~ /^SQL error: ParserError/) print "SQL error: ParserError - other"
        else if (error_msg ~ /^SQL compilation error: unsupported feature/) print "SQL compilation error: unsupported feature"
        else if (error_msg ~ /^SQL compilation error/) print "SQL compilation error: other"
        else if (error_msg ~ /^SQL error/) print "SQL error: other"
        else if (error_msg ~ /^Error during planning/) print "Error during planning: other"
        else if (error_msg ~ /^External error: Feature Arrow datatype Null is not supported/) print "External error: Arrow datatype Null not supported"
        else if (error_msg ~ /^External error: Failed to deserialize JSON/) print "External error: JSON deserialization failed"
        else if (error_msg ~ /^External error/) print "External error: other"
        else if (error_msg ~ /^Schema error: No field named .*\. Valid fields are/) print "Schema error: field not found in schema"
        else if (error_msg ~ /^Schema error/) print "Schema error: other"
        else print "Other error"
    }
}' | sort | uniq -c | sort -nr | awk '$1 >= 2' >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write Total top 10 errors (full messages)
echo "# Total top 10 errors (full messages)" >> assets/top_errors.txt
grep -A1 "Database Error\|Compilation Error" assets/run.log | grep -v "^--" | grep -v "Database Error\|Compilation Error" | sed 's/^[[:space:]]*[0-9]*: //' | sed 's/^[[:space:]]*//' | sort | uniq -c | sort -nr | awk '$1 >= 2' | head -n 10 >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write Total top 10 errors (truncated to 50 chars)
echo "# Total top 10 errors (truncated to 50 chars)" >> assets/top_errors.txt
grep -A1 "Database Error\|Compilation Error" assets/run.log | grep -v "^--" | grep -v "Database Error\|Compilation Error" | sed 's/^[[:space:]]*[0-9]*: //' | sed 's/^[[:space:]]*//' | awk '{print substr($0, 1, 50)}' | sort | uniq -c | sort -nr | awk '$1 >= 2' | head -n 10 >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Add summary of categorized vs uncategorized errors
echo "# Error categorization summary" >> assets/top_errors.txt
DBT_TOTAL_ERRORS=$(grep "Done. PASS=" assets/run.log | tail -n 1 | awk -F'ERROR=' '{print $2}' | awk -F' ' '{print $1}')
TOTAL_ERROR_LINES=$(grep -A1 "Database Error\|Compilation Error" assets/run.log | grep -v "^--" | grep -v "Database Error\|Compilation Error" | wc -l)

echo "DBT total errors: $DBT_TOTAL_ERRORS" >> assets/top_errors.txt
echo "Total error lines analyzed: $TOTAL_ERROR_LINES" >> assets/top_errors.txt
echo "Coverage: $TOTAL_ERROR_LINES out of $DBT_TOTAL_ERRORS error lines captured" >> assets/top_errors.txt