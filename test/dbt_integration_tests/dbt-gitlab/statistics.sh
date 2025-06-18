#!/bin/bash

# Clear the output file
> assets/top_errors.txt

# Write DBT Run Summary
grep "Done. PASS=" assets/run.log | tail -n 1 | awk -F'PASS=| WARN=| ERROR=| SKIP=| TOTAL=' '{print "# DBT Run Summary\nPASS: "$2"\nWARN: "$3"\nERROR: "$4"\nSKIP: "$5"\nTOTAL: "$6"\n"}' >> assets/top_errors.txt

# Write Total top 10 errors
echo "# Total top 10 errors" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:' assets/run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 10 >> assets/top_errors.txt

# Add a blank line for separation
echo "" >> assets/top_errors.txt

# Write Top function errors
echo "# Top function errors" >> assets/top_errors.txt
grep '^[[:space:]][[:space:]]000200:.*Invalid function' assets/run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 15 >> assets/top_errors.txt