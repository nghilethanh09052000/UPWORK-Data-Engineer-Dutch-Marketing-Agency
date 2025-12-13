#!/bin/bash
# Run all agency scrapers in parallel using background processes

set -e

AGENCIES=(
    "randstad"
    "tempo_team"
    "youngcapital"
    "asa_talent"
    "manpower"
    "adecco"
    "olympia"
    "start_people"
    "covebo"
    "brunel"
    "yacht"
    "maandag"
    "hays"
    "michael_page"
    "tmi"
)

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "📊 Running ${#AGENCIES[@]} scrapers in parallel..."
echo ""

# Create log directory
LOG_DIR="./scraper_logs"
mkdir -p "$LOG_DIR"

# Function to run a scraper
run_scraper() {
    local agency=$1
    local job_name="${agency}_scrape_job"
    local log_file="${LOG_DIR}/${agency}.log"
    
    echo "🚀 Starting ${agency} scraper..."
    
    uv run dagster job execute \
        -m staffing_agency_scraper.definitions \
        -j "$job_name" \
        > "$log_file" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ ${agency} completed successfully${NC}"
        return 0
    else
        echo -e "${RED}❌ ${agency} failed (exit code: $exit_code)${NC}"
        echo "   Check log: $log_file"
        return 1
    fi
}

# Run all scrapers in background (limit to 4 concurrent)
PIDS=()
FAILED=()
SUCCEEDED=()
MAX_CONCURRENT=15
RUNNING=0

for agency in "${AGENCIES[@]}"; do
    # Wait if we've reached max concurrent
    while [ $RUNNING -ge $MAX_CONCURRENT ]; do
        sleep 1
        # Check which processes are still running
        RUNNING=0
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                RUNNING=$((RUNNING + 1))
            fi
        done
    done
    
    # Start scraper in background
    run_scraper "$agency" &
    PID=$!
    PIDS+=($PID)
    RUNNING=$((RUNNING + 1))
done

# Wait for all processes to complete
echo ""
echo "⏳ Waiting for all scrapers to complete..."
for pid in "${PIDS[@]}"; do
    wait "$pid"
    EXIT_CODE=$?
    
    # Find which agency this PID belongs to (simplified - in real scenario you'd track this)
    # For now, we'll just wait and check logs
done

# Check results from logs
echo ""
echo "============================================================"
echo "📈 SCRAPING SUMMARY"
echo "============================================================"

for agency in "${AGENCIES[@]}"; do
    log_file="${LOG_DIR}/${agency}.log"
    if [ -f "$log_file" ]; then
        if grep -q "Successfully executed" "$log_file" 2>/dev/null || [ $? -ne 2 ]; then
            SUCCEEDED+=("$agency")
        else
            FAILED+=("$agency")
        fi
    else
        FAILED+=("$agency")
    fi
done

echo -e "${GREEN}✅ Succeeded: ${#SUCCEEDED[@]}/${#AGENCIES[@]}${NC}"
if [ ${#SUCCEEDED[@]} -gt 0 ]; then
    echo "   ${SUCCEEDED[*]}"
fi

echo ""
echo -e "${RED}❌ Failed: ${#FAILED[@]}/${#AGENCIES[@]}${NC}"
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "   ${FAILED[*]}"
    echo ""
    echo "Check logs in: $LOG_DIR"
    exit 1
fi

echo "============================================================"
echo -e "${GREEN}🎉 All scrapers completed successfully!${NC}"

