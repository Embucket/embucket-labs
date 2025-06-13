{%- macro sales_segment_cleaning(column_1) -%}

CASE WHEN LOWER({{ column_1 }}) = 'smb' THEN 'SMB'
     WHEN LOWER({{ column_1 }}) LIKE ('mid%market') THEN 'Mid-Market'
     WHEN LOWER({{ column_1 }}) = 'unknown' THEN 'SMB'
     WHEN LOWER({{ column_1 }}) IS NULL THEN 'SMB'
     WHEN LOWER({{ column_1 }}) = 'pubsec' THEN 'PubSec'
     WHEN LOWER({{ column_1 }}) = 'mm' THEN 'Mid-Market'
     WHEN LOWER({{ column_1 }}) = 'lrg' THEN 'Large'
     WHEN LOWER({{ column_1 }}) = 'jihu' THEN 'JiHu'
     WHEN {{ column_1 }} IS NOT NULL THEN initcap({{ column_1 }})
END

{%- endmacro -%}
