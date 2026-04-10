# Testing Insert Operation

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `spark`
 * **Last Run:** `2026-04-10 17:13:44`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        | dp_key                               |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|--------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ba592161-a31d-4865-bae2-1bfd9afcbba7 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | 37b29c2a-fc11-4494-954e-37ade7c0f999 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ca0970d3-82ff-4c50-a5f9-0b131fc55058 |



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Dimensional Table `dim_person`**


| dp_key   | id   | first_name   | last_name   | city   | email   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   | dp_created_at   | dp_replaced_at   |
|----------|------|--------------|-------------|--------|---------|--------------|------------|----------------|----------------|-----------------|------------------|

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Assertion Error

```
DataFrame are different

DataFrame shape mismatch
[left]:  (0, 12)
[right]: (3, 12)
```

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        | dp_key                               |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|--------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ba592161-a31d-4865-bae2-1bfd9afcbba7 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | 37b29c2a-fc11-4494-954e-37ade7c0f999 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ca0970d3-82ff-4c50-a5f9-0b131fc55058 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | c2d3e3fd-8115-49f5-9c32-578afc2d23d9 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | 55cfb6d6-707f-40c1-8d0a-67386664994f |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | 535b9b74-73f0-4535-8d92-9b0baf5be738 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | 49d7c976-03d9-462b-be1a-f79976f43f1f |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | c2d3e3fd-8115-49f5-9c32-578afc2d23d9 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 55cfb6d6-707f-40c1-8d0a-67386664994f |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 535b9b74-73f0-4535-8d92-9b0baf5be738 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 49d7c976-03d9-462b-be1a-f79976f43f1f |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key   | id   | first_name   | last_name   | city   | email   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   | dp_created_at   | dp_replaced_at   |
|----------|------|--------------|-------------|--------|---------|--------------|------------|----------------|----------------|-----------------|------------------|

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Assertion Error

```
DataFrame are different

DataFrame shape mismatch
[left]:  (0, 12)
[right]: (4, 12)
```

