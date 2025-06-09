#!/bin/bash

# Clear the output file
> top_errors.txt

# get total number of the last run
grep "Done. PASS=" run.log | tail -n 1 | awk -F'PASS=| WARN=| ERROR=| SKIP=| TOTAL=' '{print "# DBT Run Summary\nPASS: "$2"\nWARN: "$3"\nERROR: "$4"\nSKIP: "$5"\nTOTAL: "$6"\n"}' > top_errors.txt && echo "# Total top 10 errors" >> top_errors.txt && grep '^[[:space:]][[:space:]]000200:' run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 10 >> top_errors.txt && echo "" >> top_errors.txt && echo "# Top function errors" >> top_errors.txt && grep '^[[:space:]][[:space:]]000200:.*Invalid function' run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 15 >> top_errors.txt

# Write top10 errors with header
echo "# Total top10 errors" >> top_errors.txt
grep '^[[:space:]][[:space:]]000200:' run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 10 >> top_errors.txt

# Add a blank line for separation
echo "" >> top_errors.txt

# Write top function errors with header
echo "# Top functions errors" >> top_errors.txt
grep '^[[:space:]][[:space:]]000200:.*Invalid function' run.log | awk -F'000200: ' '{print $2}' | sort | uniq -c | sort -nr | head -n 15 >> top_errors.txt