# Testing Insert Operation

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-29 20:16:22`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                            | status   | dp_ts_from          | dp_loaded_at        |
|------|------------------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | user_info                                                                                            | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 5b5bac2f-9573-14ea-23df-ca781b0e2fbb |    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | 6299ED89FEEB382C27779740151CF032871576EE501D60D7AF0D86883115E037 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | e683c742-63f3-6271-64ed-b9a8b4c5cb32 |    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | 5DC05104E356B34E7063F9B4B1AF0D08A49392E1D18F01252BD370CB3BFB867A | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | af503a22-602c-adb7-cf8d-ca124df6fcf8 |    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | 2D7BE7D73577B6A7373E485EF76FD98B1B5C05F1140B3C12C8C13A5741DA0C41 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                                                               | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>5b5bac2f-9573-14ea-23df-ca781b0e2fbb</span> | <span style='color: green;'>1</span> | <span style='color: green;'>{'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>e683c742-63f3-6271-64ed-b9a8b4c5cb32</span> | <span style='color: green;'>2</span> | <span style='color: green;'>{'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>af503a22-602c-adb7-cf8d-ca124df6fcf8</span> | <span style='color: green;'>3</span> | <span style='color: green;'>{'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

### Assertion Error

```
DataFrame.iloc[:, 1] (column name="user_info") are different

DataFrame.iloc[:, 1] (column name="user_info") values are different (100.0 %)
[index]: [0, 1, 2]
[left]:  [{'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}, {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}, {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}]
[right]: [(Alice, Meyer, Zurich, alice.meyer@example.com), (Bob, Keller, Bern, bob.keller@example.com), (Clara, Schmid, Basel, clara.schmid@example.com)]
At positional index 0, first diff: {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'} != ('Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com')
```

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                            | status   | dp_ts_from          | dp_loaded_at        |
|------|------------------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|   10 | {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | user_info                                                                                           | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | e4059a02-b48e-fc90-b4e8-42b7ad0de301 |   10 | {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'} | D97BBA13E99644B617E27769E05867876D272182D4F218A157ABC05D13F7AD6D | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                                                                              | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 5b5bac2f-9573-14ea-23df-ca781b0e2fbb                                    | 1                                     | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e683c742-63f3-6271-64ed-b9a8b4c5cb32                                    | 2                                     | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| af503a22-602c-adb7-cf8d-ca124df6fcf8                                    | 3                                     | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>e4059a02-b48e-fc90-b4e8-42b7ad0de301</span> | <span style='color: green;'>10</span> | <span style='color: green;'>{'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

### Assertion Error

```
DataFrame.iloc[:, 1] (column name="user_info") are different

DataFrame.iloc[:, 1] (column name="user_info") values are different (100.0 %)
[index]: [0, 1, 2, 3]
[left]:  [{'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}, {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}, {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}, {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}]
[right]: [(Alice, Meyer, Zurich, alice.meyer@example.com), (Bob, Keller, Bern, bob.keller@example.com), (Clara, Schmid, Basel, clara.schmid@example.com), (Kevin, Loosli, Bern, kevin.loosli@example.com)]
At positional index 0, first diff: {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'} != ('Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com')
```

