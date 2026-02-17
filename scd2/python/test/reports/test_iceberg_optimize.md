# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Iceberg Metadata before OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165727_00044_7kr8c-8c4c4d99-48b3-4f84-b340-65e68109c680.parquet |              3 |                 1245 |
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165726_00042_7kr8c-b1923664-1810-47d1-915d-2eed1393e7e1.parquet |              3 |                 1257 |
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165727_00043_7kr8c-d027237a-7159-404a-88b1-dffb2bab335e.parquet |              3 |                 1233 |
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165725_00040_7kr8c-375628d4-fe89-4f62-a4de-7726c3c144e5.parquet |              3 |                 1237 |
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165726_00041_7kr8c-732faa8c-4a21-4b6f-9dab-b898a346c766.parquet |              3 |                 1247 |

Executing OPTIMIZE on the Iceberg table.


**### Iceberg Metadata after OPTIMIZE**


| file_path                                                                                                                                                                                   |   record_count |   file_size_in_bytes |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------------------|
| s3a://admin-bucket/warehouse/raw_person-f595c1e8d3894d78a6461c0e24631002/data/dp_loaded_at=2026-01-01T00%3A00%3A00/20260217_165728_00046_7kr8c-bd7cd64d-89bd-4569-a9ef-667a5dbcdf68.parquet |             15 |                 1648 |

