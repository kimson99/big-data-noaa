# Prerequisites

- Docker
- Ubuntu (using 24.04)
- Java 8 to run the MapReduce
- Python 3.12

`From org.cn2`

# Docker

- cd to `/src/main/java/org/cn2/docker`
- `docker compose -f docker-compose-distributed-local.yml up -d`
- Add this for namespace in `/etc/hosts`

```
127.0.0.1	localhost   namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 zknode1 hbase-master hbase-region
::1             localhost   namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 zknode1 hbase-master hbase-region
```

# Download data

Either run

- `python3 scripts/download.py` (bit slow)

or

- `./scripts/download.sh'` (using aria2, might need to install it)

# HDFS

- Copy data to `docker/data/csv`
- Go into docker interactive mode `sudo docker exec namenode -it /bin/bash`
- Run `hdfs dfs -mkdir /data` if to create directory
- Run `hdfs dfs -put hadoop_data_input/csv/*.csv.gz /data/` to copy files to HDFS

Can run the job after doing all the above

# Station Metadata

- Create `/resources/data` folder in `/main` directory
- Download `ghcnd-stations.csv` and put in `/resources/data`.
- Run `StationMetadataInitializer` to import station metadata to HBase

# Streamlit UI

- From `/ui`
- Install packages `pip install -r requirements.txt`
- `streamlit run app.py`
