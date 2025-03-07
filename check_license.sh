#!/bin/sh

LICENSE_HEADER="// Licensed to the Apache Software Foundation (ASF)"

for file in "$@"; do
    echo "$file"
    if ! grep -L "$LICENSE_HEADER" "$file"; then
        echo "License header not found in $file"
        exit 1
    fi
done
echo "LICENSE headers OK."
exit 0