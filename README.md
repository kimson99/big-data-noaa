# Prerequisites
- Docker
- Ubuntu (using 24.04)
- Java 8 to run the MapReduce

From org.cn2
# Docker
- `docker compose -f docker/docker-compose-distributed-local.yml`
- Add this for namespace in `/etc/hosts`
```
127.0.0.1	localhost   namenode datanode1 datanode2 datanode3
::1             localhost namenode datanode1 datanode2 datanode3
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
