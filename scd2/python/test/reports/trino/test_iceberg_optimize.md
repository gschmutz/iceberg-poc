# Testing Insert Operation

This test validates an INSERT operation of one new record


 * **Strategy:** `trino`
 * **Last Run:** `2026-04-10 13:35:33`
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-2bcbcbd43a564499b71091f1576c7be6/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260410_113534_02278_dk2k5-56d8262b-effc-465f-a9ee-afda4eedd4dc.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-2bcbcbd43a564499b71091f1576c7be6/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260410_113534_02276_dk2k5-6bea00ac-549c-4607-a7d2-eb2642f2c81b.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-2bcbcbd43a564499b71091f1576c7be6/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260410_113534_02279_dk2k5-9a2aa4b2-e9e5-401d-8cfb-92e222619b6f.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-2bcbcbd43a564499b71091f1576c7be6/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260410_113534_02277_dk2k5-d0e6fcdd-4c6a-468e-a2a9-346242947dee.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-2bcbcbd43a564499b71091f1576c7be6/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260410_113533_02275_dk2k5-8ba4f150-85a1-4201-8cfa-21b68541ef9a.parquet |              3 |                 1433 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                              |   record_count |   file_size_in_bytes |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-10T00%3A00%3A00/20260410_113424_02269_dk2k5-26690bb3-39af-425d-b36a-105f8fc318c1.parquet |              1 |                 1146 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260410_113402_02242_dk2k5-79c76da8-6f4a-42a8-abf5-3de913ae71d3.parquet |              2 |                 2417 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260410_113408_02251_dk2k5-104c8f15-0aee-422a-8903-1aab1641391c.parquet |              1 |                 1147 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260410_113414_02260_dk2k5-dba46cc4-faba-46b5-9628-44de7dbec10c.parquet |              1 |                 2457 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-10T00%3A00%3A00/20260410_113414_02260_dk2k5-21599f4d-a864-4678-8ec8-bac191428205.parquet |              1 |                 2457 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260410_113414_02260_dk2k5-b7595e5e-fbca-44b0-8430-0e246a28e428.parquet |              1 |                 1147 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260410_113408_02251_dk2k5-312567d2-dc9a-4037-93f8-7a32fa27171d.parquet |              1 |                 2457 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260410_113408_02251_dk2k5-4730a664-5aeb-4466-b0ec-7c129a55682b.parquet |              1 |                 2467 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-20T00%3A00%3A00/20260410_113424_02269_dk2k5-b7e6f076-f4db-49a8-a68c-c72273b8427c.parquet |              1 |                 2498 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-10T00%3A00%3A00/20260410_113424_02269_dk2k5-80369799-453c-46ae-82d6-32e8975a3f15.parquet |              1 |                 2457 |

