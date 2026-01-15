# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1
### Iceberg Metadata before OPTIMIZE

| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084928_00096_zx26p-b1fa716f-1415-40ea-b1fd-884cfe39cdd8.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084927_00094_zx26p-773b1e81-7318-453d-879a-2fd19ff0ae8d.parquet |              3 |                 1251 |
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084928_00095_zx26p-4ed23107-b4de-4dba-9b9d-6e63d07fca4b.parquet |              3 |                 1261 |
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084927_00093_zx26p-e765a24a-41cc-42a3-95f9-b34aaa259b7c.parquet |              3 |                 1241 |
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084928_00097_zx26p-09395b5c-13ac-4288-bdc1-b5f4cdb3f050.parquet |              3 |                 1249 |

Executing OPTIMIZE on the Iceberg table.
### Iceberg Metadata after OPTIMIZE

| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-19c76d339bd648c082babc34d3a6b4c8/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260115_084929_00099_zx26p-a966d436-da0d-4c21-b453-96ef5d080632.parquet |             15 |                 1659 |

