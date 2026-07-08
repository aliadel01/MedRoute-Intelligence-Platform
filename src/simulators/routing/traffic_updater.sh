#!/bin/bash
# traffic_updater.sh
# Runs inside the OSRM container alongside osrm-routed
# Watches for new traffic.csv and customizes OSRM when it changes

TRAFFIC_FILE="/data/traffic.csv"
LAST_HASH_FILE="/data/traffic.hash"
OSRM_DATA="/data/nyc-roads.osrm"
POLL_INTERVAL=30   # check every 30 seconds

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] traffic_updater: $2"; }

log "INFO" "Traffic updater started — polling every ${POLL_INTERVAL}s."

while true; do
    sleep "$POLL_INTERVAL"

    # Skip if traffic file doesn't exist yet
    if [ ! -f "$TRAFFIC_FILE" ]; then
        log "WARN" "Traffic file not found yet — waiting."
        continue
    fi

    # Compute current hash and compare to last applied
    CURRENT_HASH=$(md5sum "$TRAFFIC_FILE" | awk '{print $1}')
    OLD_HASH=$(cat "$LAST_HASH_FILE" 2>/dev/null || echo "none")

    if [ "$CURRENT_HASH" = "$OLD_HASH" ]; then
        continue   # no change — skip
    fi

    log "INFO" "New traffic data detected — customizing OSRM..."

    TMP_DIR="/tmp/osrm_build"
    rm -rf "$TMP_DIR" && mkdir -p "$TMP_DIR"

    cp /data/nyc-roads.osrm* "$TMP_DIR/"

    if osrm-customize "$TMP_DIR/nyc-roads.osrm" \
        --segment-speed-file "$TRAFFIC_FILE" > /tmp/customize_output.log 2>&1; then

        grep -i "updating\|segments\|edges\|speed" /tmp/customize_output.log | \
        while read -r line; do 
            log "INFO" "osrm-customize stat: $line"
        done

        cp "$TMP_DIR"/nyc-roads.osrm* /data/

        if pkill -HUP osrm-routed; then
            echo "$CURRENT_HASH" > "$LAST_HASH_FILE"
            log "INFO" "OSRM reloaded with updated speeds."
        else
            log "ERROR" "SIGHUP failed — trying SIGUSR1."
            pkill -USR1 osrm-routed || log "ERROR" "All reload signals failed."
        fi
        rm -rf "$TMP_DIR" /tmp/customize_output.log
    else
        log "ERROR" "osrm-customize failed! Detailed Exception below:"
        echo "==================== OSRM EXCEPTION ===================="
        cat /tmp/customize_output.log
        echo "========================================================"
        
        rm -rf "$TMP_DIR" /tmp/customize_output.log
    fi
done