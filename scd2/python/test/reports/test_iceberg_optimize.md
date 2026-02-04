# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164816_00922_z9bbh-23e53b10-e9b2-4712-a486-016e8e14b4fb.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164816_00920_z9bbh-bc928bb1-fe92-4bf5-bbc7-04bfa01c4880.parquet |              3 |                 1251 |
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164816_00921_z9bbh-8e87fe0c-20e2-40ec-a53c-41dc738dd393.parquet |              3 |                 1261 |
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164816_00919_z9bbh-642ebc9b-9b22-42b6-aaaf-c33cbe671b26.parquet |              3 |                 1241 |
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164817_00923_z9bbh-99eed8f7-c74c-41b0-8ff4-02b58351df45.parquet |              3 |                 1249 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-1cfd96baf6a44920bd4294526ffe8ff8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_164817_00925_z9bbh-ec21b81b-4abc-4065-90d7-4c608236ed2f.parquet |             15 |                 1659 |

