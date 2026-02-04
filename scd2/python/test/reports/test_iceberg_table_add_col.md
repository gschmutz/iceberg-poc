# Testing Insert Operation

This test validates an INSERT operation of one new record
## Test Step 1


**### Table raw_person before ADD COLUMN**


|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |

Executing ADD COLUMN


**### Table raw_person after ADD COLUMN**


|   id | first_name   | last_name   | city   | email                    | new_col   | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|-----------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  |           | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   |           | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com |           | ACTIVE   | 2026-01-01 00:00:00 |

## Test Step 2


**### Table raw_person**


|   id | first_name   | last_name   | city   | email                    | new_col   | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|-----------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | New Value | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | New Value | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | New Value | ACTIVE   | 2026-01-01 00:00:00 |

## Test Step 3


**### Table raw_person**


|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |

