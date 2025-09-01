#!/bin/bash

# Set the path to assets first
path_to_assets="dbt-snowplow-web/assets"

# Clear the output file
> $path_to_assets/top_errors.txt

# Write DBT Run Summary
grep "Done. PASS=" $path_to_assets/run.log | tail -n 1 | awk -F'PASS=| WARN=| ERROR=| SKIP=| TOTAL=' '{print "# DBT Run Summary\nPASS: "$2"\nWARN: "$3"\nERROR: "$4"\nSKIP: "$5"\nTOTAL: "$6"\n"}' >> $path_to_assets/top_errors.txt

# Write Total top 10 errors
echo "# Total top 10 errors" >> $path_to_assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' $path_to_assets/run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 10 >> $path_to_assets/top_errors.txt

# Add a blank line for separation
echo "" >> $path_to_assets/top_errors.txt

# Write Top function errors
echo "# Top function errors" >> $path_to_assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:.*Invalid function' $path_to_assets/run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 15 >> $path_to_assets/top_errors.txt