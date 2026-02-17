# Testing Insert Operation (Delta Mode for Source)

This test validates an INSERT operation of one single entity into an empty dimension table.
## Test Step 1
At 2026-01-01 00:00:00, insert 1 entity into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city   | email                   | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|
|           1 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| id                                   | first_name                               | last_name                                | city                                      | email                                                      | dp_valid_from                                          | dp_valid_to                                            | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|--------------------------------------|------------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

