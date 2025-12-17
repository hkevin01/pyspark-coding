#!/bin/bash

# 🔥 PySpark Monitoring Dashboard Launch Script
# This script sets up, checks, and launches the complete monitoring stack

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="/home/kevin/Projects/pyspark-coding"
FRONTEND_DIR="$PROJECT_ROOT/src/monitoring_frontend"

# Function to print colored output
print_header() {
    echo -e "\n${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_step() {
    echo -e "${PURPLE}▶ $1${NC}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a port is in use
port_in_use() {
    lsof -i ":$1" >/dev/null 2>&1
}

# Function to wait for a service to be ready
wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=30
    local attempt=1
    
    print_step "Waiting for $name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" >/dev/null 2>&1; then
            print_success "$name is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    print_error "$name failed to start after $max_attempts attempts"
    return 1
}

# Main script starts here
print_header "🚀 PySpark Monitoring Dashboard Launch Script"

# Step 1: Check prerequisites
print_header "📋 Step 1: Checking Prerequisites"

print_step "Checking required commands..."
MISSING_DEPS=0

if command_exists docker; then
    print_success "Docker is installed"
else
    print_error "Docker is not installed"
    MISSING_DEPS=1
fi

if command_exists docker-compose; then
    print_success "Docker Compose is installed"
else
    print_error "Docker Compose is not installed"
    MISSING_DEPS=1
fi

if command_exists npm; then
    print_success "npm is installed ($(npm --version))"
else
    print_error "npm is not installed"
    MISSING_DEPS=1
fi

if command_exists node; then
    print_success "Node.js is installed ($(node --version))"
else
    print_error "Node.js is not installed"
    MISSING_DEPS=1
fi

if command_exists google-chrome || command_exists google-chrome-stable || command_exists chromium-browser || command_exists chromium; then
    print_success "Chrome/Chromium is installed"
    if command_exists google-chrome; then
        CHROME_CMD="google-chrome"
    elif command_exists google-chrome-stable; then
        CHROME_CMD="google-chrome-stable"
    elif command_exists chromium-browser; then
        CHROME_CMD="chromium-browser"
    else
        CHROME_CMD="chromium"
    fi
else
    print_warning "Chrome/Chromium not found, will try xdg-open instead"
    CHROME_CMD="xdg-open"
fi

if [ $MISSING_DEPS -eq 1 ]; then
    print_error "Please install missing dependencies and try again"
    exit 1
fi

# Step 2: Check file structure
print_header "📁 Step 2: Verifying File Structure"

print_step "Checking required files..."
REQUIRED_FILES=(
    "$PROJECT_ROOT/docker-compose.monitoring.yml"
    "$FRONTEND_DIR/package.json"
    "$FRONTEND_DIR/next.config.js"
    "$FRONTEND_DIR/pages/index.tsx"
    "$PROJECT_ROOT/MONITORING_SETUP_GUIDE.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        print_success "Found: $(basename $file)"
    else
        print_error "Missing: $file"
        exit 1
    fi
done

# Step 3: Check and handle port conflicts
print_header "🔌 Step 3: Checking Port Availability"

PORTS=(3000 9080 9090 8081 8082 8083 4040 7077)
PORT_NAMES=("Dashboard" "Spark Master" "Prometheus" "Worker 1" "Worker 2" "Worker 3" "App UI" "Spark Cluster")

print_step "Checking for port conflicts..."
CONFLICTS=0
for i in "${!PORTS[@]}"; do
    port=${PORTS[$i]}
    name=${PORT_NAMES[$i]}
    if port_in_use $port; then
        print_warning "Port $port ($name) is already in use"
        CONFLICTS=1
    else
        print_success "Port $port ($name) is available"
    fi
done

if [ $CONFLICTS -eq 1 ]; then
    print_warning "Some ports are in use. Attempting to clean up..."
    cd "$PROJECT_ROOT"
    docker-compose -f docker-compose.monitoring.yml down 2>/dev/null || true
    sleep 3
fi

# Step 4: Install npm dependencies
print_header "�� Step 4: Installing npm Dependencies"

cd "$FRONTEND_DIR"
if [ ! -d "node_modules" ]; then
    print_step "Installing npm packages (this may take a few minutes)..."
    npm install
    print_success "npm packages installed successfully"
else
    print_info "node_modules already exists, checking for updates..."
    npm install
    print_success "Dependencies are up to date"
fi

# Step 5: Start Docker services
print_header "🐳 Step 5: Starting Docker Services"

cd "$PROJECT_ROOT"
print_step "Starting Spark cluster, Prometheus, and monitoring dashboard..."
docker-compose -f docker-compose.monitoring.yml up -d

print_success "Docker services started"

# Step 6: Wait for services to be ready
print_header "⏳ Step 6: Waiting for Services to Initialize"

wait_for_service "http://localhost:9080" "Spark Master" || exit 1
wait_for_service "http://localhost:9090" "Prometheus" || exit 1
wait_for_service "http://localhost:3000" "Monitoring Dashboard" || exit 1

# Step 7: Verify services
print_header "✅ Step 7: Verifying Services"

print_step "Checking Spark Master..."
if curl -s http://localhost:9080 | grep -q "Spark Master"; then
    print_success "Spark Master is running"
else
    print_warning "Spark Master may not be fully initialized"
fi

print_step "Checking Prometheus..."
if curl -s http://localhost:9090/-/healthy | grep -q "Prometheus"; then
    print_success "Prometheus is healthy"
else
    print_warning "Prometheus may not be fully initialized"
fi

print_step "Checking Workers..."
WORKERS_UP=$(docker ps --filter "name=spark-worker" --format "{{.Names}}" | wc -l)
print_info "Found $WORKERS_UP worker(s) running"

# Step 8: Display service URLs
print_header "🌐 Step 8: Service URLs"

echo -e "${GREEN}Dashboard:${NC}        http://localhost:3000"
echo -e "${GREEN}Spark Master UI:${NC}  http://localhost:9080"
echo -e "${GREEN}Prometheus:${NC}       http://localhost:9090"
echo -e "${GREEN}Worker 1 UI:${NC}      http://localhost:8081"
echo -e "${GREEN}Worker 2 UI:${NC}      http://localhost:8082"
echo -e "${GREEN}Worker 3 UI:${NC}      http://localhost:8083"
echo -e "${GREEN}App UI:${NC}           http://localhost:4040 ${YELLOW}(when job running)${NC}"

# Step 9: Launch Chrome
print_header "🌍 Step 9: Launching Dashboard in Chrome"

print_step "Opening http://localhost:3000 in Chrome..."
sleep 2

if [ "$CHROME_CMD" = "xdg-open" ]; then
    xdg-open "http://localhost:3000" &
    print_success "Dashboard opened in default browser"
else
    $CHROME_CMD "http://localhost:3000" &
    print_success "Dashboard opened in Chrome"
fi

# Step 10: Show helpful tips
print_header "💡 Quick Tips"

echo -e "${CYAN}Dashboard Features:${NC}"
echo -e "  • Toggle auto-refresh in the header"
echo -e "  • Choose refresh interval: 2s, 5s, 10s, or 30s"
echo -e "  • View cluster overview with 4 metric cards"
echo -e "  • Monitor workers in real-time"
echo -e "  • Track active applications"
echo -e "  • Visualize performance trends"

echo -e "\n${CYAN}Test the Dashboard:${NC}"
echo -e "  ${YELLOW}docker exec spark-master /opt/spark/bin/spark-submit \\${NC}"
echo -e "  ${YELLOW}    --master spark://spark-master:7077 \\${NC}"
echo -e "  ${YELLOW}    /opt/spark-apps/long_running_demo.py${NC}"

echo -e "\n${CYAN}View Logs:${NC}"
echo -e "  ${YELLOW}docker-compose -f docker-compose.monitoring.yml logs -f${NC}"

echo -e "\n${CYAN}Stop Services:${NC}"
echo -e "  ${YELLOW}docker-compose -f docker-compose.monitoring.yml down${NC}"

echo -e "\n${CYAN}Restart Services:${NC}"
echo -e "  ${YELLOW}docker-compose -f docker-compose.monitoring.yml restart${NC}"

# Final success message
print_header "🎉 SUCCESS! Monitoring Dashboard is Ready!"

print_success "All systems operational"
print_info "Dashboard is running at: http://localhost:3000"
print_info "Press Ctrl+C to view this script's log, or close this window"

# Keep script running to show any errors
echo -e "\n${BLUE}Monitoring logs (Ctrl+C to exit)...${NC}\n"
docker-compose -f docker-compose.monitoring.yml logs -f
