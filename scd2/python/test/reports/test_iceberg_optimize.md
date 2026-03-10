# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220401_01236_ptuq5-33a686d1-4966-4ed5-9315-1f5bf166ea5d.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220402_01238_ptuq5-197ed499-fdcb-4a31-bcc9-e4ee9868e461.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220401_01237_ptuq5-f1234b69-3b68-4370-a849-049e0ba9ccac.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220402_01239_ptuq5-ff13f653-4e83-4005-ba38-ded6f24a9f73.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220401_01235_ptuq5-5388f3c4-81b1-4550-a3c8-574db5fb4224.parquet |              3 |                 1433 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-9bd5d23747454609a98ec6b5c515645d/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_220402_01241_ptuq5-ada380e1-0f64-4d2f-89e4-0ea0f0a14554.parquet |             15 |                 1840 |

