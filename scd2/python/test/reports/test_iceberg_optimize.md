# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035307_03409_7kr8c-088a1008-fb9c-4bee-a9aa-879da1cc0649.parquet |              3 |                 1449 |
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035308_03411_7kr8c-249ce4dd-412d-43a1-a425-9f5609900eee.parquet |              3 |                 1435 |
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035307_03410_7kr8c-72b104c7-ef54-498c-b749-f601ad4d8cc4.parquet |              3 |                 1459 |
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035306_03408_7kr8c-afc38a4a-0481-4070-b8a9-31057ff0eee6.parquet |              3 |                 1439 |
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035308_03412_7kr8c-dec87e94-5324-4fcf-9dcb-c4ec1de2fa28.parquet |              3 |                 1447 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-e4c770b2172e4fa083894ab553ff34ba/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260218_035308_03414_7kr8c-75b872a6-8a9b-4164-804b-adfe017826c0.parquet |             15 |                 1849 |

