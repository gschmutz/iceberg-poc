# Testing Filling a gap with the same value as the version preceding and following the gap

This test validates filling the gap in a single entity. The record added into the gap is having the same values as the version before and after the gap.
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
| 91d82c01-6829-47f4-bd2a-acfb99a448fc |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 42aa1da0-54f9-4678-98e2-038d97cba7c2 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 35f98911-ecab-42a0-bc3b-8d49a40a7fab |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-11 00:00:00 |
| e3a0b3d8-4f97-475f-9889-562aa2d6a0e2 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-11 00:00:00 | 9999-12-31 23:59:59 |

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
| 35f98911-ecab-42a0-bc3b-8d49a40a7fab | 35f98911-ecab-42a0-bc3b-8d49a40a7fab |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | UPDATE_VERSION   | CASE_26     | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
| e3a0b3d8-4f97-475f-9889-562aa2d6a0e2 | e3a0b3d8-4f97-475f-9889-562aa2d6a0e2 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | DELETE_VERSION   | CASE_26     | NaT                 | NaT                 |                |                |



**Dimensional Table `dim_person`**


| dp_key                                                                | id                                 | first_name                             | last_name                               | city                                   | email                                                     | dp_ts_from                                           | dp_ts_to                                                | dp_is_active                             | dp_is_latest                             | dp_created_at                                        | dp_replaced_at                                          |
|-----------------------------------------------------------------------|------------------------------------|----------------------------------------|-----------------------------------------|----------------------------------------|-----------------------------------------------------------|------------------------------------------------------|---------------------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------------------|---------------------------------------------------------|
| 91d82c01-6829-47f4-bd2a-acfb99a448fc                                  | 1.0                                | Alice                                  | Meyer                                   | Zurich                                 | alice.meyer@example.com                                   | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| 42aa1da0-54f9-4678-98e2-038d97cba7c2                                  | 2.0                                | Bob                                    | Keller                                  | Bern                                   | bob.keller@example.com                                    | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| 35f98911-ecab-42a0-bc3b-8d49a40a7fab                                  | 3.0                                | Clara                                  | Schmid                                  | Basel                                  | clara.schmid@example.com                                  | 2026-01-01 00:00:00                                  | <span style='color: orange;'>9999-12-31 23:59:59</span> | <span style='color: orange;'>True</span> | <span style='color: orange;'>True</span> | 2026-01-02 00:00:00                                  | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color:gray;'>e3a0b3d8-4f97-475f-9889-562aa2d6a0e2</span> | <span style='color:gray;'>3</span> | <span style='color:gray;'>Clara</span> | <span style='color:gray;'>Schmid</span> | <span style='color:gray;'>Basel</span> | <span style='color:gray;'>clara.schmid@example.com</span> | <span style='color:gray;'>2026-01-10 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>2026-01-11 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
Fill the gap at 2026-01-05 00:00:00 by adding a record with the same values as the version preceding and following the gap.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-15 00:00:00 |

