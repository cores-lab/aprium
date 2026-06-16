#!/bin/bash

# --- CONFIGURATION ---
LEO_DEBUG="~moritz/astera/leo-sdk-c/examples/leo_debug"
BDF="01:00.0"
CSV_FILE="cxl_bandwidth.csv"
# ---------------------

# Safety Check: Ensure the script is run as root
if [ "$EUID" -ne 0 ]; then
  echo "Error: Please run this script with sudo."
  exit 1
fi

# Write CSV Header tracking the explicit units
echo "Timestamp,Link0_Bytes/s,Link1_Bytes/s,Total_MB/s" > "$CSV_FILE"

echo "Logging CXL bandwidth for BDF $BDF into $CSV_FILE..."
echo "Press Ctrl+C to stop."

while true; do
    # Run the SDK debug tool
    RAW_OUT=$(~moritz/astera/leo-sdk-c/examples/leo_debug -bdf "$BDF" -sample 1 -cxlstats 2>/dev/null)
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
    
    # Parse individual links (in Bytes/s)
    LINK0=$(echo "$RAW_OUT" | grep "Link 0 bandwidth:" | awk '{print $(NF-1)}')
    LINK1=$(echo "$RAW_OUT" | grep "Link 1 bandwidth:" | awk '{print $(NF-1)}')
    
    # Parse the tool's calculated aggregate value directly (in MB/s)
    TOTAL_MB=$(echo "$RAW_OUT" | grep "Total bandwidth:" | awk '{print $(NF-1)}')
    
    # Fallbacks to prevent script breakage if a link drops
    LINK0=${LINK0:-0}
    LINK1=${LINK1:-0}
    TOTAL_MB=${TOTAL_MB:-0.00}
    
    # Quick string fix if the float outputs as '.50' instead of '0.50'
    [[ $TOTAL_MB == .* ]] && TOTAL_MB="0$TOTAL_MB"
    
    # Append to the CSV file
    echo "$TIMESTAMP,$LINK0,$LINK1,$TOTAL_MB" >> "$CSV_FILE"
done
