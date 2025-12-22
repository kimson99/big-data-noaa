# Project Run Guide

This guide provides corrected steps to run the project.

## 1. Prerequisites

- **Java 8**: Required for Hadoop/HBase compatibility.
- **Python 3.12**: For the UI and scripts.
- **Docker**: For running the distributed system locally.
- **Maven**: To build the Java project.

## 2. Environment Setup

### Hosts File

Add the following to your `/etc/hosts` file (requires sudo):

```
127.0.0.1	localhost namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 zoo hbase-master hbase-region
::1             localhost namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 zoo hbase-master hbase-region
```

## 3. Start Docker

The Docker Compose file is located deep in the source tree. Run it from the project root:

```bash
docker compose -f src/main/java/org/cn2/docker/docker-compose-distributed-local.yml up -d
```

Check status: `docker ps`

## 4. Build Project

Return to the project root and build the JAR:

```bash
mvn clean install -DskipTests
```

## 5. HDFS Setup

Ensure your data is in HDFS. The code expects data at `/station/` directory.

1. Enter NameNode:
   ```bash
   docker exec -it namenode /bin/bash
   ```
2. Upload data (assuming you mapped folders correctly or have data inside container):
   ```bash
   hdfs dfs -mkdir -p /station
   hdfs dfs -put /hadoop_data_input/csv/*.csv.gz /station/
   ```

## 6. Run Analytic Jobs (Using Python CLI)

Use the **`run_noaa.py`** script to execute the MapReduce jobs or start the UI.

```bash
python3 run_noaa.py
```

Select **Option 1 (Run Job)** and then execute the steps in order:

1. **StationMetadataInitializer** (Sets up tables).
2. **Upload HBase Dependencies** (Critical Step to fix FileNotFound errors).
3. **WeatherDriverHBase** (Processes raw weather data).
4. **GlobalTrendDriver** (Calculates global trends).

## 7. Run the UI (Using Python CLI)

You can also launch the UI directly from the CLI:

```bash
python3 run_noaa.py
# Select Option 2
```

## Troubleshooting

- **FileNotFoundException (hbase-server-\*.jar)**: Run Job > Option 2 in `run_noaa.py`.
- **Connection Refused**: Check `/etc/hosts` and ensure `hbase-thrift` is up on port 9090.
- **Diagnostics**: Run `python3 run_noaa.py` and select **Option 3 (Troubleshooting)** to check containers and files.
