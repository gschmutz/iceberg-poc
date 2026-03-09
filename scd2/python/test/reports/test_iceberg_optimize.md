# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071500_05159_9p9g6-807cc773-20ae-438b-97cf-9807568336f1.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071500_05157_9p9g6-a82cec54-00f0-42e8-9068-5096ca82f59d.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071459_05156_9p9g6-4bdb5f20-d2a9-4de5-8706-7fcdb23d159c.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071500_05158_9p9g6-67db81f5-abc6-461e-bced-5db39fd31568.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071459_05155_9p9g6-20795e9d-8c51-43a9-a34e-7aca4f3a5e52.parquet |              3 |                 1433 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-488948cf76a5458abc6c938239514779/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_071501_05161_9p9g6-96ae623f-5905-463d-bb43-b9a36136b3d6.parquet |             15 |                 1843 |

