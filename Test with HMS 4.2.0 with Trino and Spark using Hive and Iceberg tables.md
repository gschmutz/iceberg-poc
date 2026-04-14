# Test with HMS 4.2.0 with Trino and Spark using Hive and Iceberg tables

## Trino

### Hive Table over Trino and HMS Thrift

```sql
CREATE TABLE minio.default.hive_table (
    id BIGINT,
    name VARCHAR,
    ts TIMESTAMP
)
WITH (
    format = 'PARQUET'
);

insert into minio.default.hive_table values (1, 'Peter Muster', TIMESTAMP '2026-02-05 10:00:00');

SELECT * FROM minio.default.hive_table;
```

### Iceberg Table over Trino and HMS Thrift

```sql
CREATE TABLE iceberg_hive.default.iceberg_table_thrift (
    id BIGINT,
    name VARCHAR,
    ts TIMESTAMP
);

insert into iceberg_hive.default.iceberg_table_thrift values (1, 'Peter Muster', TIMESTAMP '2026-02-05 15:00:00.000000');

SELECT * FROM iceberg_hive.default.iceberg_table_thrift;
```

### Iceberg Table over Trino and HMS REST API

```sql
CREATE TABLE iceberg_hive_rest.default.iceberg_table_rest (
    id BIGINT,
    name VARCHAR,
    ts TIMESTAMP
);

insert into iceberg_hive_rest.default.iceberg_table_rest values (1, 'Peter Muster', TIMESTAMP '2026-02-05 15:00:00.000000');

SELECT * FROM iceberg_hive_rest.default.iceberg_table_rest;
```

## Spark

### Hive table over Spark using HMS Thrift

```bash
docker exec -ti spark-master spark-sql
```

```
USE hive;

CREATE TABLE default.hive_table_spark (
    id BIGINT,
    name STRING,
    ts TIMESTAMP
)
STORED AS PARQUET;

insert into default.hive_table_spark 
values (1, 'Peter Muster', TIMESTAMP '2026-02-05 15:00:00.000000');

SELECT * FROM default.hive_table_spark;
```

```bash
java.lang.RuntimeException: Failed to get table info from metastore default.hive_table_spark
	at org.apache.iceberg.hive.HiveTableOperations.doRefresh(HiveTableOperations.java:124)
	at org.apache.iceberg.BaseMetastoreTableOperations.refresh(BaseMetastoreTableOperations.java:88)
	at org.apache.iceberg.BaseMetastoreTableOperations.current(BaseMetastoreTableOperations.java:71)
	at org.apache.iceberg.BaseMetastoreCatalog.loadTable(BaseMetastoreCatalog.java:49)
   ...
Caused by: org.apache.thrift.TApplicationException: Invalid method name: 'get_table'
	at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:79)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.recv_get_table(ThriftHiveMetastore.java:1514)
	at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.get_table(ThriftHiveMetastore.java:1500)
	at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getTable(HiveMetaStoreClient.java:1346)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at 
```

### Hive table over Spark using HMS REST Catalog API

```
USE hiverest;

CREATE TABLE default.iceberg_table_spark (
    id BIGINT,
    name STRING,
    ts TIMESTAMP
)
USING iceberg;

insert into default.iceberg_table_spark 
values (1, 'Peter Muster', TIMESTAMP '2026-02-05 15:00:00.000000');

SELECT * FROM default.iceberg_table_spark;
```

