# Testing Insert Operation

This test validates an INSERT operation of one new record
 * **Strategy:** `trino`
 * **Last Run:** `2026-03-25 11:59:44`
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-5fcc6501d8e741f4bc8e7979b9ac09de/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260325_105945_00074_parzf-8881cc8c-922e-4f42-9894-e2cec507ab49.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-5fcc6501d8e741f4bc8e7979b9ac09de/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260325_105945_00073_parzf-14253d7c-f563-4e4e-b437-8d3530b77b52.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-5fcc6501d8e741f4bc8e7979b9ac09de/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260325_105944_00072_parzf-4ff1de6a-b69c-41e1-8aa6-c6b3a18928e2.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-5fcc6501d8e741f4bc8e7979b9ac09de/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260325_105946_00075_parzf-344a6aef-b59f-4076-8f35-8f6b67aa3ba0.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-5fcc6501d8e741f4bc8e7979b9ac09de/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260325_105944_00071_parzf-37fb5078-36c1-48ec-8c32-ad439002d184.parquet |              3 |                 1433 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                              |   record_count |   file_size_in_bytes |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260325_104913_00064_parzf-32bb9ef8-77cd-4ca4-9e23-3ba3f48692c9.parquet |              1 |                 1147 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260325_104856_00055_parzf-6fedea48-a2ed-4743-9e68-0c94ed793ed3.parquet |              3 |                 2535 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260325_104913_00064_parzf-6ba5d70f-cb40-40df-ab79-c8f9c7fac580.parquet |              1 |                 2473 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260325_104913_00064_parzf-3d31b603-cfbe-4c9e-b6bd-128aed72324a.parquet |              1 |                 2472 |

