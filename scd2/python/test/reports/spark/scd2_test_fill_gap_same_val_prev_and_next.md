# Testing Filling a gap with the same value as the version preceding and following the gap

This test validates filling the gap in a single entity. The record added into the gap is having the same values as the version before and after the gap.


 * **Strategy:** `spark`
 * **Last Run:** `2026-04-26 19:12:30`
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


| dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 203064ea-63c6-46e5-81e5-bac5b0dbb2a9 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 402666dc-7080-416b-8071-67fe565b87ca |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 20883845-5a0a-4963-baec-1c22bd712189 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-11 00:00:00 |
| 55e0b9da-2787-4fe3-ab0f-69ee86ff7e49 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-11 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

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


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_ts               | status   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 20883845-5a0a-4963-baec-1c22bd712189 | 20883845-5a0a-4963-baec-1c22bd712189 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | UPDATE_VERSION   | CASE_26     | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
| 55e0b9da-2787-4fe3-ab0f-69ee86ff7e49 | 55e0b9da-2787-4fe3-ab0f-69ee86ff7e49 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-15 00:00:00 | ACTIVE   | DELETE_VERSION   | CASE_26     | NaT                 | NaT                 |                |                |



**Dimensional Table `dim_person`**


| dp_record_id                                                          | id                                 | first_name                             | last_name                               | city                                   | email                                                     | dp_ts_from                                           | dp_ts_to                                                | dp_is_active                             | dp_is_latest                             | dp_created_at                                        | dp_replaced_at                                          |
|-----------------------------------------------------------------------|------------------------------------|----------------------------------------|-----------------------------------------|----------------------------------------|-----------------------------------------------------------|------------------------------------------------------|---------------------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------------------|---------------------------------------------------------|
| 203064ea-63c6-46e5-81e5-bac5b0dbb2a9                                  | 1.0                                | Alice                                  | Meyer                                   | Zurich                                 | alice.meyer@example.com                                   | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| 402666dc-7080-416b-8071-67fe565b87ca                                  | 2.0                                | Bob                                    | Keller                                  | Bern                                   | bob.keller@example.com                                    | 2026-01-01 00:00:00                                  | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                  | 9999-12-31 23:59:59                                     |
| 20883845-5a0a-4963-baec-1c22bd712189                                  | 3.0                                | Clara                                  | Schmid                                  | Basel                                  | clara.schmid@example.com                                  | 2026-01-01 00:00:00                                  | <span style='color: orange;'>9999-12-31 23:59:59</span> | <span style='color: orange;'>True</span> | <span style='color: orange;'>True</span> | 2026-01-02 00:00:00                                  | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color:gray;'>55e0b9da-2787-4fe3-ab0f-69ee86ff7e49</span> | <span style='color:gray;'>3</span> | <span style='color:gray;'>Clara</span> | <span style='color:gray;'>Schmid</span> | <span style='color:gray;'>Basel</span> | <span style='color:gray;'>clara.schmid@example.com</span> | <span style='color:gray;'>2026-01-10 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>True</span>    | <span style='color:gray;'>2026-01-11 00:00:00</span> | <span style='color:gray;'>9999-12-31 23:59:59</span>    |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

