# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172556_01691_z9bbh-b65e2212-0419-481d-8300-ea52490c1e15.parquet |              3 |                 1241 |
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172556_01693_z9bbh-fdae4691-b589-4ed9-b91e-4fb761c2b067.parquet |              3 |                 1261 |
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172556_01694_z9bbh-8e140397-28a1-4018-9223-b2327e329b9a.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172556_01692_z9bbh-b568e52e-18d9-45df-ae0a-afd16611662d.parquet |              3 |                 1251 |
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172557_01695_z9bbh-80574068-cfbc-420f-8d81-7cd03f4c28de.parquet |              3 |                 1249 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-d9b04d0745ee418aac9551558c00d42b/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260204_172557_01697_z9bbh-f1486949-9454-4943-8c2a-1693a40afb55.parquet |             15 |                 1647 |

