#!/bin/bash

BASE_URL="https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/"
LIST_FILE="file_list.txt"
OUTPUT_DIR="../../../../resources/data/alt"

mkdir -p $OUTPUT_DIR       # <--- NEW: Create the folder

echo "Fetching file list from $BASE_URL..."

curl -s $BASE_URL | \
grep -o 'href="[^"]*\.csv\.gz"' | \
sed 's/href="//;s/"//' > temp_filenames.txt

awk -v base="$BASE_URL" '{print base $0}' temp_filenames.txt > $LIST_FILE
rm temp_filenames.txt

NUM_FILES=$(wc -l < $LIST_FILE)
echo "Found $NUM_FILES files. Downloading to $OUTPUT_DIR..."

aria2c -i $LIST_FILE \
       -x16 -s16 -j4 \
       -d $OUTPUT_DIR \
       --continue=true \
       --file-allocation=none

echo "Download complete."
