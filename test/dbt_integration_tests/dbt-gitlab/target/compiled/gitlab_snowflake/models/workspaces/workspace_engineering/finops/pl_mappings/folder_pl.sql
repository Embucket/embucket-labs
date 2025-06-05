-- this is the pl allocation for gcp projects under these folders

SELECT * FROM EMBUCKET.seed_engineering.gcp_billing_folder_pl_mapping
UNPIVOT(allocation FOR type IN (free, internal, paid))