## Iceberg SCD Requirements

- Join with NULL columns (when concat PK)
- All values in PK
- late but still in order
- late arrival with SAME as later 
- late arrival with corrections - DONE
- late arrival only updates until next change - DONE
- late arrival only updates until next delete
- different timestamp in source (each row has its own ts)

 


```
CREATE TABLE unity.default.delta_tbl (id INT) USING delta;
```

CREATE TABLE unity.default.t2  (id INT)
USING delta
LOCATION 's3://admin-bucket/delta/unity/default/t2';