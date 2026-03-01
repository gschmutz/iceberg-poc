# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235316_05800_fhb2g-a2c5a4c2-38c9-4ddb-b514-e99dc935f384.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235316_05799_fhb2g-31a0eb8c-1f8a-4202-b7fe-5b0bbd70762e.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235317_05801_fhb2g-2530c786-d879-42cd-a2fb-adfc479535fa.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235316_05798_fhb2g-4215a515-f62e-4c45-9e56-aa6514bb233d.parquet |              3 |                 1433 |
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235317_05802_fhb2g-12539f7b-4a0d-47a6-a0d3-493cf76b77b3.parquet |              3 |                 1441 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-bc24e037c017488d971920f318cd1e74/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260228_235317_05804_fhb2g-3b87e86b-7d3a-4b39-8de5-0c4aa75852c1.parquet |             15 |                 1842 |

