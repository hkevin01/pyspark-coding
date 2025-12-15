#!/bin/bash

################################################################################
# PyQt5 GUI Launcher for PySpark Practice System
################################################################################
# Quick launcher for all PyQt5 GUI applications
#
# Usage:
#   ./launch_gui.sh              # Show menu
#   ./launch_gui.sh batch        # Launch batch ETL GUI
#   ./launch_gui.sh kafka        # Launch Kafka streaming GUI
#   ./launch_gui.sh parquet      # Launch Kafka-to-Parquet GUI
################################################################################

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Clear screen
clear

echo ""
echo -e "${CYAN}${BOLD}================================================================================${RESET}"
echo -e "${CYAN}${BOLD}                    🚀 PYSPARK PYQT5 GUI LAUNCHER 🚀                          ${RESET}"
echo -e "${CYAN}${BOLD}================================================================================${RESET}"
echo ""

# Function to launch a GUI
launch_gui() {
    local gui_name=$1
    local gui_path="${SCRIPT_DIR}/${gui_name}"
    
    if [ ! -f "$gui_path" ]; then
        echo -e "${RED}❌ Error: ${gui_name} not found!${RESET}"
        exit 1
    fi
    
    echo -e "${GREEN}▶️  Launching ${gui_name}...${RESET}"
    echo ""
    python3 "$gui_path"
}

# Check for command line argument
if [ $# -eq 0 ]; then
    # No arguments - show menu
    echo -e "${BOLD}Choose Your GUI:${RESET}"
    echo ""
    echo -e "${GREEN}1. 📊 Batch ETL GUI${RESET}              ${CYAN}(Beginner - Green Theme)${RESET}"
    echo -e "${YELLOW}2. 🌊 Kafka Streaming GUI${RESET}        ${CYAN}(Intermediate - Amber Theme)${RESET}"
    echo -e "${PURPLE}3. 💾 Kafka-to-Parquet GUI${RESET}       ${CYAN}(Advanced - Purple Theme)${RESET}"
    echo -e "${CYAN}4. 🚪 Exit${RESET}"
    echo ""
    
    read -p "$(echo -e ${YELLOW}Choose [1-4]: ${RESET})" choice
    
    case $choice in
        1)
            launch_gui "batch_etl_gui.py"
            ;;
        2)
            launch_gui "kafka_streaming_gui.py"
            ;;
        3)
            launch_gui "kafka_to_parquet_gui.py"
            ;;
        4)
            echo ""
            echo -e "${GREEN}👋 Happy Learning!${RESET}"
            echo ""
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Invalid choice!${RESET}"
            exit 1
            ;;
    esac
else
    # Command line argument provided
    case $1 in
        batch|1)
            launch_gui "batch_etl_gui.py"
            ;;
        kafka|stream|2)
            launch_gui "kafka_streaming_gui.py"
            ;;
        parquet|lake|3)
            launch_gui "kafka_to_parquet_gui.py"
            ;;
        help|--help|-h)
            echo -e "${BOLD}Usage:${RESET}"
            echo "  ./launch_gui.sh           # Show interactive menu"
            echo "  ./launch_gui.sh batch     # Batch ETL GUI"
            echo "  ./launch_gui.sh kafka     # Kafka streaming GUI"
            echo "  ./launch_gui.sh parquet   # Kafka-to-Parquet GUI"
            echo ""
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${RESET}"
            echo "Run './launch_gui.sh help' for usage"
            exit 1
            ;;
    esac
fi
