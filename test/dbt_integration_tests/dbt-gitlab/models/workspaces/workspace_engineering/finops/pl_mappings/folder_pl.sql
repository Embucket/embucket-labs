-- this is the pl allocation for gcp projects under these folders

SELECT * FROM {{ ref ('gcp_billing_folder_pl_mapping') }}
UNPIVOT(allocation FOR type IN (free, internal, paid))