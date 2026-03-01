# Testing Insert Operation with a Composite Key (with NULL values)

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |   nan | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id1 |   id2 | first_name   | last_name   | city   | email                    | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|-------|-------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
|             |          |     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |     2 |   nan | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |     2 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id1                                  | id2                                    | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|----------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>9019ae8d-3eb7-4647-a6fb-6f73436acc68</span> | <span style='color: green;'>1</span> | <span style='color: green;'>1.0</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>132bcded-23b8-4663-a0aa-0ed4d13c1c49</span> | <span style='color: green;'>2</span> | <span style='color: green;'>3.0</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>b4e6f322-95e3-48cf-961b-ef1d6325652e</span> | <span style='color: green;'>2</span> | <span style='color: green;'>nan</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |   nan | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     2 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     2 |   nan | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    10 |    10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id1 |   id2 | first_name   | last_name   | city   | email                    | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|-------|-------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
|             |          |    10 |    10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id1                                   | id2                                     | first_name                               | last_name                                 | city                                    | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|---------------------------------------|-----------------------------------------|------------------------------------------|-------------------------------------------|-----------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 9019ae8d-3eb7-4647-a6fb-6f73436acc68                                    | 1                                     | 1.0                                     | Alice                                    | Meyer                                     | Zurich                                  | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 132bcded-23b8-4663-a0aa-0ed4d13c1c49                                    | 2                                     | 3.0                                     | Clara                                    | Schmid                                    | Basel                                   | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| b4e6f322-95e3-48cf-961b-ef1d6325652e                                    | 2                                     | <span style='color: orange;'>nan</span> | Bob                                      | Keller                                    | Bern                                    | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>536e331b-4327-4698-99ec-f7f6312882e5</span> | <span style='color: green;'>10</span> | <span style='color: green;'>10.0</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

