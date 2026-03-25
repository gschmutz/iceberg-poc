# Testing Filling a gap with the same value as the version preceding and following the gap

This test validates filling the gap in a single entity. The record added into the gap is having the same values as the version before and after the gap.
 * **Strategy:** `trino`
 * **Last Run:** `2026-03-25 12:05:12`
At 2026-01-01 00:00:00, insert 3 records, at 2026-01-05 00:00:00 delete the one with id=3 and reinsert id=3 at 2026-01-10 00:00:00 into raw table and perform initial SCD2 merge.
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| beccfee4-8be8-4e2d-bdbc-7724e8c30d63 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| e1ef2e9a-5bb6-4c1f-8350-e38448c34781 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 75f08a7d-0ef8-489a-b231-5240bbb7d3bb |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-11 00:00:00 |
| 57e97200-f4bc-4fd8-afdb-0ab1bd28c1a9 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-11 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
Fill the gap at 2026-01-05 00:00:00 by adding a record with the same values as the version preceding and following the gap.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-15 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 57e97200-f4bc-4fd8-afdb-0ab1bd28c1a9 | 57e97200-f4bc-4fd8-afdb-0ab1bd28c1a9 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | DELETE_VERSION   | CASE_26     | NaT                 |                     |                |                |
| 75f08a7d-0ef8-489a-b231-5240bbb7d3bb | 75f08a7d-0ef8-489a-b231-5240bbb7d3bb |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | UPDATE_VERSION   | CASE_26     | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                | id                                 | first_name                             | last_name                               | city                                   | email                                                     | dp_ts_from                                           | dp_ts_to                                                | dp_is_active                             | dp_is_latest                             | dp_created_at                                        | dp_replaced_at                                          |
|-----------------------------------------------------------------------|------------------------------------|----------------------------------------|-----------------------------------------|----------------------------------------|-----------------------------------------------------------|------------------------------------------------------|---------------------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------------------|---------------------------------------------------------|
| beccfee4-8be8-4e2d-bdbc-7724e8c30d63                                  | 1.0                                | Alice                                  | Meyer                                   | Zurich                                 | alice.meyer@example.com                                   | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| e1ef2e9a-5bb6-4c1f-8350-e38448c34781                                  | 2.0                                | Bob                                    | Keller                                  | Bern                                   | bob.keller@example.com                                    | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| 75f08a7d-0ef8-489a-b231-5240bbb7d3bb                                  | 3.0                                | Clara                                  | Schmid                                  | Basel                                  | clara.schmid@example.com                                  | 2026-01-01 00:00:00                                  | <span style='color: orange;'>9999-12-31 23:59:59</span> | <span style='color: orange;'>True</span> | <span style='color: orange;'>True</span> | 2026-01-02 00:00:00                                  | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color:gray;'>57e97200-f4bc-4fd8-afdb-0ab1bd28c1a9</span> | <span style='color:gray;'>3</span> | <span style='color:gray;'>Clara</span> | <span style='color:gray;'>Schmid</span> | <span style='color:gray;'>Basel</span> | <span style='color:gray;'>clara.schmid@example.com</span> | <span style='color:gray;'>2026-01-10 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>2026-01-11 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

