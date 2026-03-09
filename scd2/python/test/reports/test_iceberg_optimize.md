# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090345_06447_9p9g6-6e1fc72d-dfc0-4cc6-a971-947b09144c21.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090346_06450_9p9g6-eaf68367-e8be-42fc-9be7-7ffa91502b4a.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090344_06446_9p9g6-6f860683-38a4-4545-94d9-b98ce722c3a1.parquet |              3 |                 1433 |
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090345_06448_9p9g6-66d63a1a-a438-45dc-8898-39382fc90111.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090346_06449_9p9g6-64396082-0254-4e8c-b55e-ae1a0cfffafb.parquet |              3 |                 1429 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-543990e085f044cbbc599dd4f02a7da7/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260309_090347_06452_9p9g6-142c1b00-23bf-4e0c-a61f-7c77ce936706.parquet |             15 |                 1841 |

