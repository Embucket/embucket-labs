#!/bin/bash

# Clear the output file
> assets/top_errors.txt

# Write DBT Run Summary
grep "Done. PASS=" assets/run.log | tail -n 1 | awk -F'PASS=| WARN=| ERROR=| SKIP=| TOTAL=' '{print "# DBT Run Summary\nPASS: "$2"\nWARN: "$3"\nERROR: "$4"\nSKIP: "$5"\nTOTAL: "$6"\n"}' >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write errors grouped by high-level categories
echo "# Errors grouped by high-level categories" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{
    if ($2 ~ /^Optimizer rule/) print "Optimizer rule"
    else if ($2 ~ /^Error during planning/) print "Error during planning"
    else if ($2 ~ /^SQL compilation error/) print "SQL compilation error"
    else if ($2 ~ /^SQL error/) print "SQL error"
    else if ($2 ~ /^External error/) print "External error"
    else if ($2 ~ /^Schema error/) print "Schema error"
    else if ($2 ~ /^Arrow error/) print "Arrow error"
    else if ($2 ~ /^Threaded Job error/) print "Threaded Job error"
    else if ($2 ~ /^Invalid datatype/) print "Invalid datatype"
    else if ($2 ~ /^This feature is not implemented/) print "This feature is not implemented"
    else if ($2 ~ /^type_coercion/) print "type_coercion"
    else print "Other error"
}' | sort | uniq -c | sort -nr >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write errors grouped by type (comprehensive analysis)
echo "# Errors grouped by type (comprehensive analysis)" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{
    if ($2 ~ /^Optimizer rule .optimize_projections. failed/) print "Optimizer rule: optimize_projections"
    else if ($2 ~ /^Optimizer rule .common_sub_expression_eliminate. failed/) print "Optimizer rule: common_sub_expression_eliminate"
    else if ($2 ~ /^Optimizer rule .eliminate_cross_join. failed/) print "Optimizer rule: eliminate_cross_join"
    else if ($2 ~ /^Optimizer rule/) print "Optimizer rule: other"
    else if ($2 ~ /^Error during planning: Inserting query must have the same schema/) print "Error during planning: schema mismatch"
    else if ($2 ~ /^Error during planning: Internal error/) print "Error during planning: internal error"
    else if ($2 ~ /^Error during planning: Function .* failed to match/) print "Error during planning: function signature"
    else if ($2 ~ /^Error during planning: For function/) print "Error during planning: function type error"
    else if ($2 ~ /^Error during planning: Unexpected argument/) print "Error during planning: argument error"
    else if ($2 ~ /^Error during planning/) print "Error during planning: other"
    else if ($2 ~ /^SQL compilation error: table .* not found/) print "SQL compilation error: table not found"
    else if ($2 ~ /^SQL compilation error: column .* not found/) print "SQL compilation error: column not found"
    else if ($2 ~ /^SQL compilation error: error line/) print "SQL compilation error: syntax error"
    else if ($2 ~ /^SQL compilation error: unsupported feature/) print "SQL compilation error: unsupported feature"
    else if ($2 ~ /^SQL compilation error/) print "SQL compilation error: other"
    else if ($2 ~ /^SQL error: ParserError.*Expected: literal string/) print "SQL error: ParserError - literal string"
    else if ($2 ~ /^SQL error: ParserError.*Expected: \[EXTERNAL\] TABLE/) print "SQL error: ParserError - CREATE statement"
    else if ($2 ~ /^SQL error: ParserError/) print "SQL error: ParserError - other"
    else if ($2 ~ /^SQL error/) print "SQL error: other"
    else if ($2 ~ /^External error: Feature Arrow datatype/) print "External error: Arrow datatype"
    else if ($2 ~ /^External error/) print "External error: other"
    else if ($2 ~ /^Schema error: No field named/) print "Schema error: field not found"
    else if ($2 ~ /^Schema error/) print "Schema error: other"
    else if ($2 ~ /^Arrow error: Cast error/) print "Arrow error: cast error"
    else if ($2 ~ /^Arrow error/) print "Arrow error: other"
    else if ($2 ~ /^Threaded Job error/) print "Threaded Job error"
    else if ($2 ~ /^Invalid datatype/) print "Invalid datatype"
    else if ($2 ~ /^This feature is not implemented/) print "Feature not implemented"
    else if ($2 ~ /^type_coercion/) print "Type coercion error"
    else print "Other error"
}' | sort | uniq -c | sort -nr | awk '$1 >= 2' >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write Total top 10 errors (full messages)
echo "# Total top 10 errors (full messages)" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | awk '$1 >= 2' | head -n 10 >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write Total top 10 errors (truncated to 50 chars)
echo "# Total top 10 errors (truncated to 50 chars)" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{print substr($2, 1, 50)}' | sort | uniq -c | sort -nr | awk '$1 >= 2' | head -n 10 >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt
