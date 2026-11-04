# AKHQ

Kafka GUI for Apache Kafka to manage topics, topics data, consumers group, schema registry, connect and more... 

**[Website](https://akhq.io/)** | **[Documentation](https://akhq.io/docs/)** | **[GitHub](https://github.com/tchiotludo/akhq)**

## How to enable?

```
platys init --enable-services AKHQ
platys gen
```

## How to use it?

Navigate to <http://localhost:28345>.
If authentication is enabled, login with user `admin` and password `abc123!`.

To use the REST API <http://localhost:28345/api> (see <https://akhq.io/docs/api.html>)


### Monitoring API
  
  * <http://localhost:28320/info>  
  * <http://localhost:28320/health>
  * <http://localhost:28320/loggers>
  * <http://localhost:28320/metrics>
  * <http://localhost:28320/prometheus>