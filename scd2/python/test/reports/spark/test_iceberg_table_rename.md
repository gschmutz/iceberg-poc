# Testing Insert Operation

This test validates an INSERT operation of one new record


 * **Strategy:** `spark`
 * **Last Run:** `2026-04-10 13:52:09`
## Test Step 1


**### Table raw_person**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from                | dp_loaded_at              |
|------|--------------|-------------|--------|--------------------------|----------|---------------------------|---------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |

Executing RENAME of `raw_person` to `raw_person_renamed`


**### Table default.raw_person_renamed**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from                | dp_loaded_at              |
|------|--------------|-------------|--------|--------------------------|----------|---------------------------|---------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2025-12-31 23:00:00+00:00 | 2025-12-31 23:00:00+00:00 |

