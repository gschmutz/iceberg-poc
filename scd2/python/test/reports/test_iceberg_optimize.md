# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180210_02207_7kr8c-c2352f30-7b56-4d1e-b955-4198124a4060.parquet |              3 |                 1439 |
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180211_02210_7kr8c-47b571f2-d496-4d27-8597-cabd84d50324.parquet |              3 |                 1435 |
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180211_02211_7kr8c-a39208c3-503e-4647-84a2-5e856881d5f3.parquet |              3 |                 1447 |
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180211_02209_7kr8c-589ec8b2-ff86-4116-a052-75b6dd13ca1e.parquet |              3 |                 1459 |
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180211_02208_7kr8c-fc4ef79c-c522-4265-bbdb-8206faced470.parquet |              3 |                 1449 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-b75fa7a8fc98446da6fbe0cf8676903a/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_180212_02213_7kr8c-f8a7fbce-3e66-46db-ae7b-fc76cf262003.parquet |             15 |                 1849 |

