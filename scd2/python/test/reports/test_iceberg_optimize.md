# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213306_02183_zu3ry-0e0f4e9f-cb29-4a7d-9da3-a5e63cc0d526.parquet |              3 |                 1241 |
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213308_02185_zu3ry-7ebd7bd6-bad0-4775-8092-36c62b9b6636.parquet |              3 |                 1261 |
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213308_02186_zu3ry-4f269067-6c68-457c-927e-16ff15442381.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213306_02184_zu3ry-0a1297e8-12da-4dc0-9f59-b372dc366756.parquet |              3 |                 1251 |
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213308_02187_zu3ry-dea1f11c-12ea-467e-87cd-5017f712c8d5.parquet |              3 |                 1249 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-5e5355a6da614f658b0a3c7839b2a8bc/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260130_213309_02189_zu3ry-3f61a84a-22c4-47ec-a450-1528b2b2ba35.parquet |             15 |                 1651 |

