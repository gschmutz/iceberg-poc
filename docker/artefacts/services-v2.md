# platys-platform - List of Services

| Service | Links | External<br>Port | Internal<br>Port | Description
|--------------|------|------|------|------------
|[hive-metastore](./documentation/services/hive-metastore )|[Rest API](http://localhost:9084/iceberg/v1/namespaces)|9083<br>9084<br>|9083<br>9084<br>|Hive Metastore
|[hive-metastore-db](./documentation/services/hive-metastore )||5442<br>|5432<br>|Hive Metastore DB
|[markdown-viewer](./documentation/services/markdown-viewer )|[Web UI](http://localhost:80)|80<br>|3000<br>|Platys Platform homepage viewer
|[minio-1](./documentation/services/minio )|[Web UI](http://localhost:9010)|9000<br>9010<br>|9000<br>9010<br>|Software-defined Object Storage
|[minio-mc](./documentation/services/minio )||||MinIO Console
|[spark-master](./documentation/services/spark )|[Web UI](http://localhost:28304)|28304<br>6066<br>7077<br>4040-4044<br>|28304<br>6066<br>7077<br>4040-4044<br>|Spark Master Node
|[spark-worker-1](./documentation/services/spark )||28111<br>|28111<br>|Spark Worker Node
|[trino-1](./documentation/services/trino )|[Web UI](http://localhost:28082/ui/preview)|28082<br>28087<br>|8080<br>8443<br>|SQL Virtualization Engine
|[trino-cli](./documentation/services/trino-cli )||||Trino CLI|

**Note:** init container ("init: true") are not shown