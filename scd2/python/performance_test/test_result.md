# Test Case: Check Phyiscal Delete in Raw Table

## Step 1

Insert data into the raw table

### Table: `iceberg_hive.default.employees`
|   id | first\_name   | last\_name   | city   | email                    | status   | load\_ts   |
|------|---------------|--------------|--------|--------------------------|----------|------------|
|    1 | Alice         | Meyer        | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 |
|    2 | Bob           | Keller       | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 |
|    3 | Clara         | Schmid       | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 |

### Table: `iceberg_hive.default.view_employees_scd2`
|   merge\_key |   id | first\_name   | last\_name   | city   | email                    | load\_ts   | status   | change\_classification   | operation\_type   |
|--------------|------|---------------|--------------|--------|--------------------------|------------|----------|--------------------------|-------------------|
|            1 |    1 | Alice         | Meyer        | Zurich | alice.meyer@example.com  | 2026-01-01 | ACTIVE   | NEW                      | UPDATE_EXISTING   |
|            2 |    2 | Bob           | Keller       | Bern   | bob.keller@example.com   | 2026-01-01 | ACTIVE   | NEW                      | UPDATE_EXISTING   |
|            3 |    3 | Clara         | Schmid       | Basel  | clara.schmid@example.com | 2026-01-01 | ACTIVE   | NEW                      | UPDATE_EXISTING   |

### Table: `iceberg_hive.default.dim_employees`
|   id | first\_name   | last\_name   | city   | email                    | dp\_valid\_from     | dp\_valid\_to       | dp\_is\_active   | dp\_is\_latest   | dp\_load\_timestamp   | dp\_created\_at     | dp\_replaced\_at    | change\_type   | record\_hash                                                     |
|------|---------------|--------------|--------|--------------------------|---------------------|---------------------|------------------|------------------|-----------------------|---------------------|---------------------|----------------|------------------------------------------------------------------|
|    1 | Alice         | Meyer        | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True             | True             | 2026-01-02 00:00:00   | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | NEW            | FF118EED04F8A2D0133E79435F7BC3CEBC0011D256A07FE02953CD12B3E29E51 |
|    2 | Bob           | Keller       | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True             | True             | 2026-01-02 00:00:00   | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | NEW            | 68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE |
|    3 | Clara         | Schmid       | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True             | True             | 2026-01-02 00:00:00   | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | NEW            | 67A87A1E14991AF623E8AC26518B9BB757E481E9B47AE9CBC728833FDDCEF86E |

## Test Case 2


