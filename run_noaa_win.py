#!/usr/bin/env python3
import os
import subprocess
import sys
import shutil

# --- Configuration ---
HBASE_CONTAINER = "hbase-master"
NAMENODE_CONTAINER = "namenode"
JAR_SOURCE = "target/big-data-noaa-1.0-SNAPSHOT.jar"
JAR_DEST = "/tmp/big-data-noaa.jar"
STATION_FILE_HOST = "src/main/resources/data/ghcnd-stations.csv"

def run_command(cmd, shell=True, check=True):
    try:
        subprocess.run(cmd, shell=shell, check=check)
    except subprocess.CalledProcessError as e:
        print(f"Lỗi khi thực thi: {cmd}")
        return False
    return True

def setup_dependencies():
    print("[*] Đang thiết lập HBase dependencies trên HDFS (Sửa lỗi NameNode)...")
    temp_dir = os.path.abspath("temp_hbase_libs")
    
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    # 1. Lấy thư viện từ hbase-master về máy Windows
    print("-> Đang lấy thư viện từ hbase-master...")
    run_command(f"docker cp {HBASE_CONTAINER}:/opt/hbase-1.2.6/lib/. {temp_dir}")
    
    # 2. Đẩy thư viện vào NameNode (Nơi có lệnh hdfs)
    print("-> Đang đẩy thư viện vào NameNode...")
    run_command(f"docker exec {NAMENODE_CONTAINER} mkdir -p /tmp/hbase-libs-to-hdfs/")
    run_command(f"docker cp {temp_dir}/. {NAMENODE_CONTAINER}:/tmp/hbase-libs-to-hdfs/")
    
    # 3. Đưa lên HDFS
    print("-> Đang đưa thư viện lên HDFS...")
    run_command(f"docker exec {NAMENODE_CONTAINER} hdfs dfs -mkdir -p /opt/hbase-1.2.6/lib/")
    run_command(f"docker exec {NAMENODE_CONTAINER} hdfs dfs -put -f /tmp/hbase-libs-to-hdfs/*.jar /opt/hbase-1.2.6/lib/")
    
    print("[+] Hoàn tất thiết lập dependencies.")

def run_java_class(class_name):
    # Tự động copy JAR trước khi chạy bất kỳ Job nào
    run_command(f"docker cp {JAR_SOURCE} {HBASE_CONTAINER}:{JAR_DEST}")
    
    # Nếu là khởi tạo trạm, tự động copy file CSV vào container
    if "StationMetadataInitializer" in class_name:
        if os.path.exists(STATION_FILE_HOST):
            print(f"[*] Đang nạp file dữ liệu trạm vào {HBASE_CONTAINER}...")
            run_command(f"docker exec {HBASE_CONTAINER} mkdir -p src/main/resources/data/")
            run_command(f"docker cp {STATION_FILE_HOST} {HBASE_CONTAINER}:/src/main/resources/data/ghcnd-stations.csv")
        else:
            print(f"[!] Cảnh báo: Không tìm thấy {STATION_FILE_HOST} trên máy host.")

    print(f"[*] Đang chạy {class_name}...")
    # Dùng sh -c để xử lý biến môi trường chính xác trong container
    docker_cmd = f"docker exec -it {HBASE_CONTAINER} sh -c \"export CLASSPATH=$(hbase classpath):{JAR_DEST}; java {class_name}\""
    run_command(docker_cmd)

def main_menu():
    while True:
        print("\n" + "="*40)
        print("   NOAA BIG DATA - WINDOWS CLI (INTEGRATED)")
        print("="*40)
        print("1. Chạy TẤT CẢ các bước thiết lập (Dependencies + Metadata)")
        print("2. Chạy lẻ: StationMetadataInitializer")
        print("3. Chạy lẻ: Upload Dependencies")
        print("-" * 20)
        print("4. Chạy lẻ: Weather Data Processing (Job MapReduce chính)")
        print("5. Chạy lẻ: Global Trend Analysis")
        print("6. Chạy lẻ: Station-Year Trend Analysis")
        print("-" * 20)
        print("7. Chạy UI (Streamlit)")
        print("0. Thoát")
        
        choice = input("\nChọn công việc: ").strip()
        
        if choice == '1':
            setup_dependencies()
            run_java_class("org.cn2.station.StationMetadataInitializer")
        elif choice == '2':
            run_java_class("org.cn2.station.StationMetadataInitializer")
        elif choice == '3':
            setup_dependencies()
        elif choice == '4':
            run_java_class("org.cn2.weather.WeatherDriverHBase")
        elif choice == '5':
            run_java_class("org.cn2.trend.global.GlobalTrendDriver")
        elif choice == '6':
            run_java_class("org.cn2.trend.station.StationYearTrendDriver")
        elif choice == '7':
            ui_dir = "ui"
            subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"], cwd=ui_dir)
        elif choice == '0':
            break

if __name__ == "__main__":
    if not os.path.exists(JAR_SOURCE):
        print(f"Lỗi: Không tìm thấy file {JAR_SOURCE}. Hãy chạy 'mvn clean install' trước.")
    else:
        main_menu()