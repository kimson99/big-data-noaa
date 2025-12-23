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

## 3. Download Data

Choose one of the following options:

1.  **Data by Stations**: Information about stations (Name, Location)
2.  **Data by Year**: Daily recordings (TMIN, TMAX, etc.)

### 3.1 Data by Stations (Recommended)

This is required for the UI map to work.

```bash
cd src/main/java/org/cn2/scripts
./download-station.sh
```

### 3.2 Data by Year

You can use either Python or Shell script (using `aria2c` for speed).

**Option A: Python (Simpler)**

```bash
cd src/main/java/org/cn2/scripts
python3 download.py
```

**Option B: Shell (Faster)**
Requires `aria2c` installed.

```bash
cd src/main/java/org/cn2/scripts
./download.sh
```

## 4. Start Docker

The Docker Compose file is located deep in the source tree. Run it from the project root:

```bash
docker compose -f src/main/java/org/cn2/docker/docker-compose-distributed-local.yml up -d
```

Check status: `docker ps`

Create the destination directory:

```bash
mkdir -p src/main/java/org/cn2/docker/data/csv
```

Copy the files:

```bash
cp src/main/resources/data/station/*.csv.gz src/main/java/org/cn2/docker/data/csv/
```

## 5. Build Project

Return to the project root and build the JAR:

```bash
mvn clean install
```

## 6. HDFS Setup

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


## Docker
# Tạo thư mục đích trong container
docker exec -it hbase-master mkdir -p /opt/hbase-1.2.6/src/main/resources/data/
# Bước 1: Copy dữ liệu vào Docker : Chạy tại PowerShell Windows để đưa file vào "vùng xanh" của Java:
> docker cp src/main/resources/data/ghcnd-stations.csv hbase-master:/opt/hbase-1.2.6/src/main/resources/data/ghcnd-stations.csv
# Upload du lieu tho vao HDFS
docker exec -it namenode bash
hdfs dfs -mkdir -p /station
hdfs dfs -put /hadoop_data_input/csv/*.csv.gz /station/
# Bước 2: Chạy Job khởi tạo trạm (Metadata)
Chạy tại root@hbase-master (Docker Bash). Điểm mấu chốt là thêm thư mục resources vào đầu CLASSPATH:
```t
# Di chuyển vào thư mục làm việc
cd /opt/hbase-1.2.6

# Thiết lập Classpath bao gồm cả thư mục resources
export CLASSPATH=/opt/hbase-1.2.6/src/main/resources/:$(hbase classpath):/tmp/big-data-noaa.jar

# Thực thi lệnh nạp trạm
java org.cn2.station.StationMetadataInitializer
```
# Giai đoạn 2: Chạy MapReduce nạp dữ liệu (HBase)
```t
# Cập nhật Classpath cho MapReduce
export CLASSPATH=$(hbase mapredcp):$(hbase classpath):/tmp/big-data-noaa.jar

# Job 1: Nạp dữ liệu thời tiết chính
java org.cn2.weather.WeatherDriverHBase

# Job 2: Tính toán xu hướng toàn cầu (Key: Năm)
java org.cn2.trend.global.GlobalTrendDriver

# Job 3: Tính toán xu hướng từng trạm (Key: ID_Năm)
java org.cn2.trend.station.StationYearTrendDriver
```
# Thay 'hbase-master' bằng 'localhost' vì chạy từ ngoài Docker
connection = happybase.Connection('localhost', port=9090)

# UI
pip install streamlit happybase pandas plotly numpy thrift
streamlit run app.py

## 7. Run Analytic Jobs (Using Python CLI)
> pip install streamlit happybase pandas plotly numpy thrift
Use the **`run_noaa_win.py`** script to execute the MapReduce jobs or start the UI.
```bash
python3 run_noaa_win.py
```

Select **Option 1 (Run Job)** and then execute the steps in order:

1. **StationMetadataInitializer** (Sets up tables).
2. **Upload HBase Dependencies to HDFS**
3. **Weather Data Processing**
4. **Global Trend Analysis**
5. **Station-Year Trend Analysis**

## 8. Run the UI (Using Python CLI)

You can also launch the UI directly from the CLI:

```bash
python3 run_noaa.py

```