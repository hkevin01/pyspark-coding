#!/bin/bash

################################################################################
# PySpark ETL Practice System Launcher
################################################################################
# Quick launcher for all practice systems
#
# Usage:
#   ./run_practice.sh           # Launch main menu
#   ./run_practice.sh batch     # Run batch ETL practice
#   ./run_practice.sh kafka     # Run Kafka streaming practice
#   ./run_practice.sh parquet   # Run Kafka-to-Parquet practice
#   ./run_practice.sh launcher  # Run master launcher
################################################################################

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
RESET='\033[0m'

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Clear screen
clear

# Show banner
echo ""
echo -e "${CYAN}${BOLD}================================================================================${RESET}"
echo -e "${CYAN}${BOLD}                    🚀 PYSPARK ETL PRACTICE SYSTEM 🚀                          ${RESET}"
echo -e "${CYAN}${BOLD}================================================================================${RESET}"
echo ""

# Function to run a practice script
run_practice() {
    local script_name=$1
    local script_path="${SCRIPT_DIR}/${script_name}"
    
    if [ ! -f "$script_path" ]; then
        echo -e "${RED}❌ Error: ${script_name} not found!${RESET}"
        exit 1
    fi
    
    echo -e "${GREEN}▶️  Launching ${script_name}...${RESET}"
    echo ""
    python3 "$script_path"
}

# Check for command line argument
if [ $# -eq 0 ]; then
    # No arguments - show menu
    echo -e "${BOLD}Choose Your Practice System:${RESET}"
    echo ""
    echo -e "${CYAN}${BOLD}Terminal-based (Classic):${RESET}"
    echo -e "${GREEN}1. 📊 Batch ETL Practice${RESET}          ${CYAN}(CSV → Parquet)${RESET}"
    echo -e "${YELLOW}2. 🌊 Kafka Streaming Practice${RESET}    ${CYAN}(Kafka → Kafka)${RESET}"
    echo -e "${MAGENTA}3. 💾 Kafka-to-Parquet Practice${RESET}   ${CYAN}(Kafka → Data Lake)${RESET}"
    echo -e "${BLUE}4. 🎯 Master Launcher${RESET}             ${CYAN}(All-in-one menu)${RESET}"
    echo ""
    echo -e "${CYAN}${BOLD}PyQt5 GUIs (Visual):${RESET}"
    echo -e "${GREEN}5. 🖥️  Batch ETL GUI${RESET}              ${CYAN}(Beginner - Green)${RESET}"
    echo -e "${YELLOW}6. 🖥️  Kafka Streaming GUI${RESET}        ${CYAN}(Intermediate - Amber)${RESET}"
    echo -e "${MAGENTA}7. 🖥️  Kafka-to-Parquet GUI${RESET}       ${CYAN}(Advanced - Purple)${RESET}"
    echo -e "${BLUE}8. 🚀 Launch GUI Selector${RESET}         ${CYAN}(GUI Launcher)${RESET}"
    echo ""
    echo -e "${RED}9. 🚪 Exit${RESET}"
    echo ""
    
    read -p "$(echo -e ${YELLOW}Choose [1-9]: ${RESET})" choice
    
    case $choice in
        1)
            run_practice "batch_etl_practice.py"
            ;;
        2)
            run_practice "kafka_streaming_practice.py"
            ;;
        3)
            run_practice "kafka_to_parquet_practice.py"
            ;;
        4)
            run_practice "practice_launcher.py"
            ;;
        5)
            run_practice "batch_etl_gui.py"
            ;;
        6)
            run_practice "kafka_streaming_gui.py"
            ;;
        7)
            run_practice "kafka_to_parquet_gui.py"
            ;;
        8)
            "${SCRIPT_DIR}/launch_gui.sh"
            ;;
        9)
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
        batch)
            run_practice "batch_etl_practice.py"
            ;;
        kafka)
            run_practice "kafka_streaming_practice.py"
            ;;
        parquet)
            run_practice "kafka_to_parquet_practice.py"
            ;;
        launcher|menu)
            run_practice "practice_launcher.py"
            ;;
        gui-batch|batch-gui)
            run_practice "batch_etl_gui.py"
            ;;
        gui-kafka|kafka-gui)
            run_practice "kafka_streaming_gui.py"
            ;;
        gui-parquet|parquet-gui)
            run_practice "kafka_to_parquet_gui.py"
            ;;
        gui|guis)
            "${SCRIPT_DIR}/launch_gui.sh"
            ;;
        help|--help|-h)
            echo -e "${BOLD}Usage:${RESET}"
            echo "  ./run_practice.sh              # Show interactive menu"
            echo ""
            echo -e "${BOLD}Terminal Practice:${RESET}"
            echo "  ./run_practice.sh batch        # Batch ETL practice"
            echo "  ./run_practice.sh kafka        # Kafka streaming practice"
            echo "  ./run_practice.sh parquet      # Kafka-to-Parquet practice"
            echo "  ./run_practice.sh launcher     # Master launcher"
            echo ""
            echo -e "${BOLD}PyQt5 GUIs:${RESET}"
            echo "  ./run_practice.sh batch-gui    # Batch ETL GUI"
            echo "  ./run_practice.sh kafka-gui    # Kafka streaming GUI"
            echo "  ./run_practice.sh parquet-gui  # Kafka-to-Parquet GUI"
            echo "  ./run_practice.sh gui          # GUI launcher menu"
            echo ""
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${RESET}"
            echo "Run './run_practice.sh help' for usage"
            exit 1
            ;;
    esac
fi
