# Testing for valid data at a given at a given timestamp

This test validates a single SELECT operation for data valid at a timestamp 2026-01-03 00:00:00
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_exported_at      |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 |



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                   | dp_valid_from       | dp_valid_to         | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Perform Test
Select data valid at 2026-01-03 00:00:00. As we are selecting back in time the old version of Alice is shown where she lived in Zurich.


`
        SELECT * 
        FROM iceberg_hive.default.dim_person
        WHERE TIMESTAMP '2026-01-05 00:00:00' - INTERVAL '2' DAY BETWEEN dp_valid_from AND dp_valid_to
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                   | dp_valid_from       | dp_valid_to         | dp_is_active   | dp_is_latest   | dp_load_timestamp   | dp_created_at       | dp_replaced_at      | change_type   | record_hash                                                      |
|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|---------------------|---------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 | SUPERSEDED    | FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | NEW           | 68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE |

