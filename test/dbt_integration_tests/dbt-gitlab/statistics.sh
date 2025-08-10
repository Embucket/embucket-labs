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
    else if ($2 ~ /^This feature is not implemented/) print "Feature not implemented"
    else if ($2 ~ /^type_coercion/) print "Type coercion error"
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
    else if ($2 ~ /^Arrow: Incompatible type/) print "Arrow error: incompatible type"
    else if ($2 ~ /^Feature .* is not supported/) print "Feature not supported"
    else print "Other error"
}' | sort | uniq -c | sort -nr | awk '$1 >= 2' >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write non-000200 errors analysis
echo "# Non-000200 errors analysis" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]Error during planning:' assets/run.log | grep -v '000200:' | awk '{
    if ($0 ~ /Function .* failed to match any signature/) print "Error during planning: function signature mismatch"
    else if ($0 ~ /Function .* expects .* but received/) print "Error during planning: function argument type mismatch"
    else if ($0 ~ /Internal error: Expect TypeSignatureClass/) print "Error during planning: internal type signature error"
    else if ($0 ~ /Window has mismatch/) print "Error during planning: window schema mismatch"
    else if ($0 ~ /Error during planning: Internal error/) print "Error during planning: internal error"
    else if ($0 ~ /Error during planning/) print "Error during planning: other"
    else print "Other non-000200 error"
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

# Add summary of categorized vs uncategorized errors
echo "# Error categorization summary" >> assets/top_errors.txt
DBT_TOTAL_ERRORS=$(grep "Done. PASS=" assets/run.log | tail -n 1 | awk -F'ERROR=' '{print $2}' | awk -F' ' '{print $1}')
TOTAL_000200_ERRORS=$(grep '^[[:space:]][[:space:]]000200:' assets/run.log | wc -l)
TOTAL_NON_000200_ERRORS=$(grep '^[[:space:]][[:space:]]Error during planning:' assets/run.log | grep -v '000200:' | wc -l)
CATEGORIZED_000200_ERRORS=$(grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{
    if ($2 ~ /^Optimizer rule .optimize_projections. failed/) count++
    else if ($2 ~ /^Optimizer rule .common_sub_expression_eliminate. failed/) count++
    else if ($2 ~ /^Optimizer rule .eliminate_cross_join. failed/) count++
    else if ($2 ~ /^Optimizer rule/) count++
    else if ($2 ~ /^Error during planning: Inserting query must have the same schema/) count++
    else if ($2 ~ /^Error during planning: Internal error/) count++
    else if ($2 ~ /^Error during planning: Function .* failed to match/) count++
    else if ($2 ~ /^Error during planning: For function/) count++
    else if ($2 ~ /^Error during planning: Unexpected argument/) count++
    else if ($2 ~ /^Error during planning/) count++
    else if ($2 ~ /^SQL compilation error: table .* not found/) count++
    else if ($2 ~ /^SQL compilation error: column .* not found/) count++
    else if ($2 ~ /^SQL compilation error: error line/) count++
    else if ($2 ~ /^SQL compilation error: unsupported feature/) count++
    else if ($2 ~ /^SQL compilation error/) count++
    else if ($2 ~ /^SQL error: ParserError.*Expected: literal string/) count++
    else if ($2 ~ /^SQL error: ParserError.*Expected: \[EXTERNAL\] TABLE/) count++
    else if ($2 ~ /^SQL error: ParserError/) count++
    else if ($2 ~ /^SQL error/) count++
    else if ($2 ~ /^External error: Feature Arrow datatype/) count++
    else if ($2 ~ /^External error/) count++
    else if ($2 ~ /^Schema error: No field named/) count++
    else if ($2 ~ /^Schema error/) count++
    else if ($2 ~ /^Arrow error: Cast error/) count++
    else if ($2 ~ /^Arrow error/) count++
    else if ($2 ~ /^Threaded Job error/) count++
    else if ($2 ~ /^Invalid datatype/) count++
    else if ($2 ~ /^This feature is not implemented/) count++
    else if ($2 ~ /^type_coercion/) count++
    else if ($2 ~ /^Arrow: Incompatible type/) count++
    else if ($2 ~ /^Feature .* is not supported/) count++
} END {print count}')
CATEGORIZED_NON_000200_ERRORS=$(grep '^[[:space:]][[:space:]]Error during planning:' assets/run.log | grep -v '000200:' | awk '{
    if ($0 ~ /Function .* failed to match any signature/) count++
    else if ($0 ~ /Function .* expects .* but received/) count++
    else if ($0 ~ /Internal error: Expect TypeSignatureClass/) count++
    else if ($0 ~ /Window has mismatch/) count++
    else if ($0 ~ /Error during planning: Internal error/) count++
    else if ($0 ~ /Error during planning/) count++
} END {print count}')
UNCATEGORIZED_000200=$((TOTAL_000200_ERRORS - CATEGORIZED_000200_ERRORS))
UNCATEGORIZED_NON_000200=$((TOTAL_NON_000200_ERRORS - CATEGORIZED_NON_000200_ERRORS))
OTHER_DBT_ERRORS=$((DBT_TOTAL_ERRORS - TOTAL_000200_ERRORS - TOTAL_NON_000200_ERRORS))
echo "DBT total errors: $DBT_TOTAL_ERRORS" >> assets/top_errors.txt
echo "000200: errors analyzed: $TOTAL_000200_ERRORS" >> assets/top_errors.txt
echo "Non-000200 errors analyzed: $TOTAL_NON_000200_ERRORS" >> assets/top_errors.txt
echo "Other DBT errors (not analyzed): $OTHER_DBT_ERRORS" >> assets/top_errors.txt
echo "Categorized 000200 errors: $CATEGORIZED_000200_ERRORS" >> assets/top_errors.txt
echo "Uncategorized 000200 errors: $UNCATEGORIZED_000200" >> assets/top_errors.txt
echo "Categorized non-000200 errors: $CATEGORIZED_NON_000200_ERRORS" >> assets/top_errors.txt
echo "Uncategorized non-000200 errors: $UNCATEGORIZED_NON_000200" >> assets/top_errors.txt
