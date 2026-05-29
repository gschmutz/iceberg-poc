# Testing for valid data at a given at a given timestamp

This test validates a single SELECT operation for data valid at a timestamp 2026-01-03 00:00:00


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-07 13:55:25`
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 5e2c7780-ed16-4bda-811d-022de48ee83d |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 8476d353-4371-4a23-aa34-0f9f0f29b454 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |
| c0ccb883-c9f6-4148-a8c3-a6946c7ed7b7 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 2a71a988-5e3b-4ee3-a83e-50153b92961c |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT id, first_name, last_name, city, email,
                dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest,
                dp_load_ts, dp_replace_ts,
                dp_record_hash  
        FROM default.dim_person
        WHERE dp_is_latest = TRUE
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       | dp_record_hash                                                   |
|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 | 59C89B7EF4747123AFC82ADCD3FC344DBFCDFAE429135184BB56925C4E542CD1 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 | E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 | 254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB |

