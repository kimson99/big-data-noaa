#!/usr/bin/env python3
import os
import subprocess
import sys
import time

# --- Configuration ---
HBASE_CONTAINER = "hbase-master"
NAMENODE_CONTAINER = "namenode"
JAR_SOURCE = "target/big-data-noaa-1.0-SNAPSHOT.jar"
JAR_DEST = "/tmp/big-data-noaa.jar"

# --- Helper Functions ---
def run_command(cmd, shell=True, check=True):
    """Runs a shell command and prints the output."""
    try:
        subprocess.run(cmd, shell=shell, check=check)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {cmd}")
        print(f"Exit code: {e.returncode}")
        # Don't exit immediately, let the caller handle it or continue
        return False
    return True

def print_header(title):
    print("\n" + "=" * 40)
    print(f"   {title}")
    print("=" * 40)

# --- Job Functions ---
def check_jar():
    if not os.path.exists(JAR_SOURCE):
        print(f"Error: JAR file not found at {JAR_SOURCE}")
        print("Please run 'mvn clean install -DskipTests' first.")
        return False
    return True

def copy_jar():
    print("[1/3] Copying JAR to container...")
    if not run_command(f"docker cp {JAR_SOURCE} {HBASE_CONTAINER}:{JAR_DEST}"):
        return False
    return True

def setup_dependencies():
    print("Setting up HBase dependencies in HDFS...")
    
    cmds = [
        # 1. Copy libs from hbase-master to local tmp
        "rm -rf /tmp/hbase-lib-copy",
        "mkdir -p /tmp/hbase-lib-copy",
        f"docker cp {HBASE_CONTAINER}:/opt/hbase-1.2.6/lib/ /tmp/hbase-lib-copy/",
        
        # 2. Copy libs to namenode
        f"docker cp /tmp/hbase-lib-copy {NAMENODE_CONTAINER}:/tmp/hbase-lib-copy/",
        
        # 3. Put into HDFS
        f"docker exec {NAMENODE_CONTAINER} bash -c 'hdfs dfs -mkdir -p /opt/hbase-1.2.6/lib/ && hdfs dfs -put -f /tmp/hbase-lib-copy/lib/*.jar /opt/hbase-1.2.6/lib/'"
    ]
    
    for cmd in cmds:
        if not run_command(cmd):
            print("Failed during dependency setup.")
            return

    print("Dependencies setup complete.")

def run_java_class(class_name):
    print(f"[2/3] Running {class_name}...")
    # Escape $() for the shell command
    cmd = f'docker exec -it {HBASE_CONTAINER} bash -c "export CLASSPATH=\\$(hbase classpath):{JAR_DEST}; java {class_name}"'
    run_command(cmd)

def menu_run_job():
    if not check_jar(): return
    if not copy_jar(): return

    while True:
        print("\n--- Run MapReduce/HBase Job ---")
        print("1) StationMetadataInitializer (HBase Setup)")
        print("2) Upload HBase Dependencies to HDFS (Fix for FileNotFoundException)")
        print("3) WeatherDriverHBase (Weather Data Processing)")
        print("4) GlobalTrendDriver (Global Trend Analysis)")
        print("0) Back to Main Menu")
        
        choice = input("Enter choice: ").strip()
        
        if choice == '1':
            run_java_class("org.cn2.station.StationMetadataInitializer")
        elif choice == '2':
            setup_dependencies()
        elif choice == '3':
            run_java_class("org.cn2.weather.WeatherDriverHBase")
        elif choice == '4':
            run_java_class("org.cn2.trend.global.GlobalTrendDriver")
        elif choice == '0':
            break
        else:
            print("Invalid choice.")

# --- UI Functions ---
def menu_run_ui():
    print_header("Starting Streamlit UI")
    ui_dir = "ui"
    if not os.path.exists(ui_dir):
        print("Error: 'ui' directory not found.")
        return

    print("Running 'streamlit run app.py' in ./ui directory...")
    try:
        # Use sys.executable to ensure we use the same python interpreter
        subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"], cwd=ui_dir)
    except KeyboardInterrupt:
        print("\nUI Stopped.")

# --- Troubleshooting Functions ---
def menu_troubleshoot():
    print_header("Troubleshooting Environment")
    
    # 1. Check Jar
    print(f"1. Checking Project JAR ({JAR_SOURCE})... ", end="")
    if os.path.exists(JAR_SOURCE):
        print("OK")
    else:
        print("MISSING (Run 'mvn clean install')")
        
    # 2. Check Docker Containers
    print("2. Checking Docker Containers...")
    required_containers = [HBASE_CONTAINER, NAMENODE_CONTAINER, "zoo"] # Keeping 'zoo' as requested
    
    for container in required_containers:
        print(f"   - {container}: ", end="")
        res = subprocess.run(f"docker ps -q -f name={container}", shell=True, capture_output=True, text=True)
        if res.stdout.strip():
            print("RUNNING")
        else:
            print("NOT RUNNING (Check user guide)")

    # 3. Check Data File
    data_file = "src/main/resources/data/ghcnd-stations.csv"
    print(f"3. Checking Station Metadata ({data_file})... ", end="")
    if os.path.exists(data_file):
        print("OK")
    else:
        print("MISSING")

    input("\nPress cmd+click to continue...")

# --- Main CLI ---
def main():
    while True:
        print_header("NOAA Big Data Project CLI")
        print("1) Run Job (MapReduce / Setup)")
        print("2) Run UI (Streamlit)")
        print("3) Troubleshooting")
        print("0) Exit")
        
        choice = input("Enter choice: ").strip()
        
        if choice == '1':
            menu_run_job()
        elif choice == '2':
            menu_run_ui()
        elif choice == '3':
            menu_troubleshoot()
        elif choice == '0':
            print("Goodbye!")
            sys.exit(0)
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
