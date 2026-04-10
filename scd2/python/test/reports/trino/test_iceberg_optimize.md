# Testing Insert Operation

This test validates an INSERT operation of one new record
 * **Strategy:** `trino`
 * **Last Run:** `2026-04-09 18:28:56`
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-140b7e6fd68f4b0bba3b74a1ac66bc0c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260409_162827_00101_hqpn9-581c4798-1139-43ce-a316-03d506cc8259.parquet |              3 |                 1453 |
| s3a://admin-bucket/warehouse/raw_person-140b7e6fd68f4b0bba3b74a1ac66bc0c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260409_162828_00103_hqpn9-d081f795-04fa-4d85-b1bf-e215c44e7125.parquet |              3 |                 1441 |
| s3a://admin-bucket/warehouse/raw_person-140b7e6fd68f4b0bba3b74a1ac66bc0c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260409_162828_00102_hqpn9-a4dd4765-eff8-4e16-8034-ed0b6f059fce.parquet |              3 |                 1429 |
| s3a://admin-bucket/warehouse/raw_person-140b7e6fd68f4b0bba3b74a1ac66bc0c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260409_162827_00100_hqpn9-eef82290-8679-4e8a-9e53-ec1b87b4f1bb.parquet |              3 |                 1443 |
| s3a://admin-bucket/warehouse/raw_person-140b7e6fd68f4b0bba3b74a1ac66bc0c/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260409_162827_00099_hqpn9-0762f56b-c60f-45b3-b1b5-cd27b27df705.parquet |              3 |                 1433 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                              |   record_count |   file_size_in_bytes |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260409_162731_00093_hqpn9-c53b7917-ffc6-4bab-a98b-2cb8430a81a1.parquet |              1 |                 1146 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260409_162731_00093_hqpn9-d61f0dae-3644-4eda-9c9a-85446336f5b8.parquet |              1 |                 2592 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-05T00%3A00%3A00/20260409_162731_00093_hqpn9-25913b66-44d6-450d-a13f-96068f6b33ee.parquet |              1 |                 2592 |
| s3a://warehouse-bucket/iceberg_poc/default/dim_person/data/dp_ts_from=2026-01-01T00%3A00%3A00/20260409_162713_00084_hqpn9-8514a44b-4f6d-4398-8245-28c2e8a1ac8c.parquet |              3 |                 2660 |

