# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-676ee3b14db84321a340f26e452fe19c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260320_212959_00158_vw8eq-af93b2a2-4d2c-48aa-b82c-2b05dbd8e3a2.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-676ee3b14db84321a340f26e452fe19c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260320_212957_00154_vw8eq-9cc2ec0d-47c8-420b-9031-92c1ba9ecac3.parquet |              3 |                 1433 |
| s3a://admin-bucket/warehouse/raw_person-676ee3b14db84321a340f26e452fe19c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260320_212959_00156_vw8eq-d2fca8f3-6f3e-49bf-a32e-0e05846b061f.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-676ee3b14db84321a340f26e452fe19c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260320_212959_00157_vw8eq-a5120fa4-a08d-42e8-bee3-12930067379b.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-676ee3b14db84321a340f26e452fe19c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260320_212958_00155_vw8eq-1f97a641-8501-4149-a8e9-f24214889298.parquet |              3 |                 1443 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                              |   record_count |   file_size_in_bytes |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260320_212939_00148_vw8eq-5d980f25-a053-40a9-99b9-33c434647cc6.parquet |              1 |                 2467 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260320_212929_00139_vw8eq-7a9ff0d6-36df-4aae-9a1a-84b64f2810ad.parquet |              3 |                 2534 |

