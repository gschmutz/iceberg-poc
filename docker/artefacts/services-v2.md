# platys-platform - List of Services

| Service | Links | External<br>Port | Internal<br>Port | Description
|--------------|------|------|------|------------
|[apicurio-registry-1](./documentation/services/apicurio-registry )|[Web UI](http://192.168.1.112:8081) - [Rest API](http://192.168.1.112:8081/apis/ccompat/v7)|8081<br>|8080<br>|Apicurio Schema Registry
|[apicurio-registry-ui](./documentation/services/apicurio-registry )|[Web UI](http://192.168.1.112:28398)|28398<br>|8080<br>|Apicurio Registry UI
|[duckdb](./documentation/services/duckdb )|[Web UI](https://192.168.1.112:28249)|28249<br>|8443<br>|Analytical in-process SQL DBMS
|[hive-metastore](./documentation/services/hive-metastore )||9083<br>9084<br>|9083<br>9084<br>|Hive Metastore
|[hive-metastore-db](./documentation/services/hive-metastore )||5442<br>|5432<br>|Hive Metastore DB
|[jupyter](./documentation/services/jupyter )|[Web UI](http://192.168.1.112:28888)|28888<br>28376-28380<br>|8888<br>4040-4044<br>|Web-based interactive development environment for notebooks, code, and data
|[kafka-1](./documentation/services/kafka )||9092<br>19092<br>29092<br>39092<br>9992<br>1234<br>|9092<br>19092<br>29092<br>39092<br>9992<br>1234<br>|Kafka Broker 1
|[kafka-2](./documentation/services/kafka )||9093<br>19093<br>29093<br>39093<br>9993<br>1235<br>|9093<br>19093<br>29093<br>39093<br>9993<br>1234<br>|Kafka Broker 2
|[kafka-3](./documentation/services/kafka )||9094<br>19094<br>29094<br>39094<br>9994<br>1236<br>|9094<br>19094<br>29094<br>39094<br>9994<br>1234<br>|Kafka Broker 3
|[lakevision](./documentation/services/lakevision )|[Web UI](http://192.168.1.112:28397)|28397<br>|8081<br>|Insights into Lakehouse
|[lakevision-worker](./documentation/services/lakevision )||||Insights into Lakehouse (background worker)
|[markdown-viewer](./documentation/services/markdown-viewer )|[Web UI](http://192.168.1.112:80)|80<br>|3000<br>|Platys Platform homepage viewer
|[minio-1](./documentation/services/minio )|[Web UI](http://192.168.1.112:9010)|9000<br>9010<br>|9000<br>9010<br>|Software-defined Object Storage
|[minio-mc](./documentation/services/minio )||||MinIO Console
|[nimtable](./documentation/services/nimtable )||28281<br>|8182<br>|Control Plane for Iceberg
|[nimtable-web](./documentation/services/nimtable )|[Web UI](http://192.168.1.112:28280)|28280<br>|3000<br>|Control Plane for Iceberg
|[postgresql](./documentation/services/postgresql )||5432<br>|5432<br>|Open-Source object-relational database system
|[spark-master](./documentation/services/spark )|[Web UI](http://192.168.1.112:28304)|28304<br>6066<br>7077<br>4040-4044<br>|28304<br>6066<br>7077<br>4040-4044<br>|Spark Master Node
|[spark-worker-1](./documentation/services/spark )||28111<br>|28111<br>|Spark Worker Node
|[trino-1](./documentation/services/trino )|[Web UI](http://192.168.1.112:28082/ui/preview)|28082<br>28087<br>|8080<br>8443<br>|SQL Virtualization Engine
|[trino-cli](./documentation/services/trino )||||Trino CLI|

**Note:** init container ("init: true") are not shown