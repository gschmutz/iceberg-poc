# Testing Insert Operation (Millisecond Granularity)

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities, using millisecond timestamp granularity.


 * **Strategy:** `pyspark`
 * **Timestamp Granularity:** `millisecond`
 * **Last Run:** `2026-10-03 15:45:09`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge with millisecond-precision timestamps.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from                 | dp_loaded_at               |
|------|--------------|-------------|--------|--------------------------|----------|----------------------------|----------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |



**Input to Merge**


|   merge_record_id | merge_dp_ts_to   | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from                 | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|----------------------------|---------------------|----------------|----------------|
|               nan | NaT              | a3e4001f-24d0-bbbb-c613-e7435a8c5cb6 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00.123000 | 9999-12-31 23:59:59 | True           | True           |
|               nan | NaT              | 01de4dca-43ff-271e-3f2d-6143ea616a71 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00.123000 | 9999-12-31 23:59:59 | True           | True           |
|               nan | NaT              | 352f7d81-4192-0bd5-b3c8-b15ea8871ddb |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00.123000 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                                    | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                                    | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|---------------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|---------------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>a3e4001f-24d0-bbbb-c613-e7435a8c5cb6</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00.123000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00.456000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>01de4dca-43ff-271e-3f2d-6143ea616a71</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00.123000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00.456000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>352f7d81-4192-0bd5-b3c8-b15ea8871ddb</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00.123000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00.456000</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00.789000, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from                 | dp_loaded_at               |
|------|--------------|-------------|--------|--------------------------|----------|----------------------------|----------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00.123000 | 2026-01-01 00:00:00.123000 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00.789000 | 2026-01-05 00:00:00.789000 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00.789000 | 2026-01-05 00:00:00.789000 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00.789000 | 2026-01-05 00:00:00.789000 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00.789000 | 2026-01-05 00:00:00.789000 |

