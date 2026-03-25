# Testing Logical Delete Operation (Delta Mode for Source)

This test validates a DELETE operation of a single entity. The delete is created by a logical delete in the raw table, i.e., the record status is set to INACTIVE. This test ensures that no further actions are taken with the same record in later partitions.
 * **Strategy:** `spark`
 * **Last Run:** `2026-03-25 13:56:39`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 26d432fa-6015-4778-b44e-d48a548fc613 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 8b6f1239-1bbb-44cf-91cc-70549da00ac7 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 3b3d6329-dd44-425f-b19f-e09cdd6c5ede |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1cf2cbf8-b92b-4a31-a1ec-4bdff81cdc75</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>6304806b-daf9-4dcf-af93-3cc1e02fb571</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>49c42d4c-8246-4470-a320-ddee97dce92c</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, update entity with `id=3` in raw table an INACTIVE (logical delete) and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 49c42d4c-8246-4470-a320-ddee97dce92c | 49c42d4c-8246-4470-a320-ddee97dce92c |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | INACTIVE | UPDATE_VERSION   | CASE_30     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to                                                | dp_is_active                              | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
| 1cf2cbf8-b92b-4a31-a1ec-4bdff81cdc75 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 6304806b-daf9-4dcf-af93-3cc1e02fb571 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 49c42d4c-8246-4470-a320-ddee97dce92c |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-02 00:00:00 | <span style='color: orange;'>2026-01-06 00:00:00</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

