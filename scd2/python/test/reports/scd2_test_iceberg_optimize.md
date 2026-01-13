# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1
### Iceberg Metadata before OPTIMIZE

| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212011_07675_y3ads-0ea4ba5b-3f37-4c31-820c-48447967c6f1.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212011_07676_y3ads-6fddfd07-bdef-48e2-8f02-eef85bc82644.parquet |              3 |                 1249 |
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212010_07673_y3ads-a04fe76e-7e22-417b-b37e-f3ad3f584714.parquet |              3 |                 1251 |
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212010_07674_y3ads-3a38703f-22dd-4162-aa70-3ca3219a6a2b.parquet |              3 |                 1261 |
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212009_07672_y3ads-1c085dfe-2421-4764-8500-ae86d013ddae.parquet |              3 |                 1241 |

Executing OPTIMIZE on the Iceberg table.
### Iceberg Metadata after OPTIMIZE

| file_path                                                                                                                                                                                     |   record_count |   file_size_in_bytes |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-178b65313af24dc28a41e5e0ca799d46/data/dp_exported_at=2026-01-01T00%3A00%3A00/20260112_212012_07678_y3ads-ff0cd387-68a4-42f3-a4e4-a0a4a0b5bcd4.parquet |             15 |                 1652 |

