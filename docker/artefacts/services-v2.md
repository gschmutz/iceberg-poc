# platys-platform - List of Services

| Service | Links | External<br>Port | Internal<br>Port | Description
|--------------|------|------|------|------------
|[duckdb](./documentation/services/duckdb )|[Web UI](https://127.0.0.1:28249)|28249<br>|8443<br>|Analytical in-process SQL DBMS
|[hive-metastore](./documentation/services/hive-metastore )||9083<br>9084<br>|9083<br>9084<br>|Hive Metastore
|[hive-metastore-db](./documentation/services/hive-metastore )||5442<br>|5432<br>|Hive Metastore DB
|[jupyter](./documentation/services/jupyter )|[Web UI](http://127.0.0.1:28888)|28888<br>28376-28380<br>|8888<br>4040-4044<br>|Web-based interactive development environment for notebooks, code, and data
|[lakevision](./documentation/services/lakevision )|[Web UI](http://127.0.0.1:28397)|28397<br>|8081<br>|Insights into Lakehouse
|[lakevision-worker](./documentation/services/lakevision )||||Insights into Lakehouse (background worker)
|[markdown-viewer](./documentation/services/markdown-viewer )|[Web UI](http://127.0.0.1:80)|80<br>|3000<br>|Platys Platform homepage viewer
|[minio-1](./documentation/services/minio )|[Web UI](http://127.0.0.1:9010)|9000<br>9010<br>|9000<br>9010<br>|Software-defined Object Storage
|[minio-mc](./documentation/services/minio )||||MinIO Console
|[nimtable](./documentation/services/nimtable )||28281<br>|8182<br>|Control Plane for Iceberg
|[nimtable-web](./documentation/services/nimtable )|[Web UI](http://127.0.0.1:28280)|28280<br>|3000<br>|Control Plane for Iceberg
|[postgresql](./documentation/services/postgresql )||5432<br>|5432<br>|Open-Source object-relational database system
|[spark-master](./documentation/services/spark )|[Web UI](http://127.0.0.1:28304)|28304<br>6066<br>7077<br>4040-4044<br>|28304<br>6066<br>7077<br>4040-4044<br>|Spark Master Node
|[spark-worker-1](./documentation/services/spark )||28111<br>|28111<br>|Spark Worker Node
|[trino-1](./documentation/services/trino )|[Web UI](http://127.0.0.1:28082/ui/preview)|28082<br>28087<br>|8080<br>8443<br>|SQL Virtualization Engine
|[trino-cli](./documentation/services/trino )||||Trino CLI|

**Note:** init container ("init: true") are not shown