#!/bin/bash

# Configuration
SOURCE_DIR="../../../../resources/data/station"
OUTPUT_DIR="../../../../resources/data/merged_input"
BLOCK_SIZE="128m" # HDFS default block size
OUTPUT_PREFIX="noaa_merged_"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Starting merge process..."
echo "Source: $SOURCE_DIR"
echo "Output: $OUTPUT_DIR (Batch size: $BLOCK_SIZE)"

# ---------------------------------------------------------
# EXPLANATION OF COMMAND:
# 1. find: Locates all .csv.gz files safely (handles spaces/newlines).
# 2. xargs + zcat: Decompresses files in a continuous stream.
# 3. awk (Optional): Filters out headers if they exist in every small file.
#    (I commented it out. Enable it if your CSVs have headers).
# 4. split: Cuts the stream into 128MB chunks, aligned to line breaks.
# ---------------------------------------------------------

find "$SOURCE_DIR" -name "*.csv.gz" -print0 | \
    xargs -0 zcat | \
    # awk 'NR==1 || $0 !~ /^ID,YEAR/' | \  # <-- UNCOMMENT THIS LINE TO REMOVE REPEATED HEADERS
    split -C "$BLOCK_SIZE" \
          --numeric-suffixes=1 \
          --additional-suffix=.csv \
          - "$OUTPUT_DIR/$OUTPUT_PREFIX"

echo "------------------------------------------------"
echo "Merge complete."
echo "Resulting files:"
ls -lh "$OUTPUT_DIR" | head -n 10
echo "..."
