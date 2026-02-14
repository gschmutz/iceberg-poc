# AKHQ

Kafka GUI for Apache Kafka to manage topics, topics data, consumers group, schema registry, connect and more... 

**[Website](https://akhq.io/)** | **[Documentation](https://akhq.io/docs/)** | **[GitHub](https://github.com/tchiotludo/akhq)**

## How to enable?

```
platys init --enable-services AKHQ
platys gen
```

## How to use it?

Navigate to <http://10.156.72.221:28107>.
If authentication is enabled, login with user `admin` and password `abc123!`.

To use the REST API <http://10.156.72.221:28107/api> (see <https://akhq.io/docs/api.html>)


### Monitoring API
  
  * <http://10.156.72.221:28320/info>  
  * <http://10.156.72.221:28320/health>
  * <http://10.156.72.221:28320/loggers>
  * <http://10.156.72.221:28320/metrics>
  * <http://10.156.72.221:28320/prometheus>