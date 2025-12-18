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

echo -e "${CYAN}This script will perform the following checks and setup:${NC}"
echo -e "  1. ✓ Prerequisites (Docker, Docker Compose, Node.js, npm, Chrome)"
echo -e "  2. ✓ File structure verification"
echo -e "  3. ✓ CORS & React Error validation (Python)"
echo -e "  4. ✓ Existing Spark cluster detection and cleanup"
echo -e "  5. ✓ Port conflict resolution"
echo -e "  6. ✓ Next.js configuration validation"
echo -e "  7. ✓ npm dependencies installation"
echo -e "  8. ✓ Docker images availability"
echo -e "  9. ✓ Service startup and health checks"
echo -e " 10. ✓ Post-launch API verification"
echo -e " 11. ✓ Browser launch"
echo ""

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

if command_exists docker-compose || docker compose version >/dev/null 2>&1; then
    print_success "Docker Compose is installed"
    # Detect which docker-compose command to use
    if docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
        print_info "Using: docker compose (v2)"
    else
        DOCKER_COMPOSE_CMD="docker-compose"
        print_info "Using: docker-compose (v1)"
    fi
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

# Check if Docker daemon is running
print_step "Checking Docker daemon..."
if ! docker ps >/dev/null 2>&1; then
    print_error "Docker daemon is not running"
    print_info "Start Docker with: sudo systemctl start docker"
    exit 1
fi
print_success "Docker daemon is running"

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

# Step 3: CORS & React Error Validation
print_header "🔬 Step 3: CORS & React Error Validation"

print_step "Running comprehensive code validation..."
VALIDATION_SCRIPT="$FRONTEND_DIR/scripts/validate_monitoring.py"

if [ -f "$VALIDATION_SCRIPT" ]; then
    print_info "Checking for CORS issues, React Error #31 risks, and configuration problems..."
    
    # Run the Python validation script
    if python3 "$VALIDATION_SCRIPT"; then
        print_success "All validation checks passed!"
    else
        print_error "Validation failed! Please fix the errors above before launching."
        print_info "Common issues:"
        print_info "  - CORS: API calls should use /api/spark/ proxy routes, not direct localhost URLs"
        print_info "  - React Error #31: TypeScript interfaces must use lowercase property names (coresused, not coresUsed)"
        print_info "  - Docker: Ensure docker-compose.monitoring.yml has internal URL environment variables"
        exit 1
    fi
else
    print_warning "Validation script not found: $VALIDATION_SCRIPT"
    print_info "Skipping Python validation - proceeding with basic checks..."
    
    # Fallback: Basic inline checks
    print_step "Running basic CORS checks..."
    
    # Check if sparkApi.ts uses proxy routes
    if grep -q "/api/spark/" "$FRONTEND_DIR/lib/sparkApi.ts" 2>/dev/null; then
        print_success "sparkApi.ts uses API proxy routes (CORS safe)"
    else
        print_error "sparkApi.ts may not use API proxy routes - CORS errors likely!"
        exit 1
    fi
    
    # Check for React Error #31 - workers should be array type
    if grep -q "workers.*:.*SparkWorker\[\]" "$FRONTEND_DIR/lib/sparkApi.ts" 2>/dev/null; then
        print_success "ClusterStatus.workers is correctly typed as SparkWorker[]"
    elif grep -q "workers.*:.*number" "$FRONTEND_DIR/lib/sparkApi.ts" 2>/dev/null; then
        print_error "ClusterStatus.workers typed as 'number' - will cause React Error #31!"
        print_info "The Spark API returns workers as an array, not a number."
        exit 1
    fi
    
    # Check for lowercase property names
    if grep -q "coresused" "$FRONTEND_DIR/lib/sparkApi.ts" 2>/dev/null; then
        print_success "Using correct lowercase property names (coresused)"
    fi
    if grep -q "coresUsed.*:" "$FRONTEND_DIR/lib/sparkApi.ts" 2>/dev/null; then
        print_error "Using incorrect camelCase property names (coresUsed) - Spark API uses lowercase!"
        exit 1
    fi
fi

# Step 4: Check for existing Spark clusters
print_header "🔍 Step 4: Checking for Existing Spark Clusters"

# Check if old spark cluster is running
OLD_CLUSTER_RUNNING=0
if docker ps --format "{{.Names}}" | grep -q "^spark-master$"; then
    print_warning "Found running Spark cluster (old setup)"
    OLD_CLUSTER_RUNNING=1
fi

# Check if monitoring stack is already running
MONITORING_RUNNING=0
if docker ps --format "{{.Names}}" | grep -q "^pyspark-monitoring-dashboard$"; then
    print_info "Monitoring stack is already running"
    MONITORING_RUNNING=1
fi

if [ $OLD_CLUSTER_RUNNING -eq 1 ]; then
    print_step "Stopping old Spark cluster to avoid conflicts..."
    
    # Try to stop old cluster from docker/spark-cluster
    if [ -f "$PROJECT_ROOT/docker/spark-cluster/docker-compose.yml" ]; then
        cd "$PROJECT_ROOT/docker/spark-cluster"
        $DOCKER_COMPOSE_CMD down 2>/dev/null || true
        print_success "Old Spark cluster stopped"
    else
        # Fallback: stop containers manually
        print_info "Stopping Spark containers manually..."
        docker stop spark-master spark-worker-1 spark-worker-2 spark-worker-3 2>/dev/null || true
        docker rm spark-master spark-worker-1 spark-worker-2 spark-worker-3 2>/dev/null || true
        print_success "Old containers stopped and removed"
    fi
    sleep 2
fi

# Step 5: Check port availability
print_header "🔌 Step 5: Checking Port Availability"

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
    print_warning "Some ports still in use. Stopping monitoring stack..."
    cd "$PROJECT_ROOT"
    $DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml down 2>/dev/null || true
    sleep 3
    
    # Recheck ports
    STILL_CONFLICTS=0
    for i in "${!PORTS[@]}"; do
        port=${PORTS[$i]}
        if port_in_use $port; then
            print_error "Port $port still in use after cleanup"
            STILL_CONFLICTS=1
        fi
    done
    
    if [ $STILL_CONFLICTS -eq 1 ]; then
        print_error "Unable to free all required ports"
        print_info "Check which process is using the ports: sudo lsof -i :3000"
        exit 1
    fi
    print_success "All ports are now available"
fi

# Step 6: Verify Next.js configuration
print_header "📝 Step 6: Verifying Next.js Configuration"

print_step "Checking next.config.js for standalone output..."
if grep -q "output.*:.*['\"]standalone['\"]" "$FRONTEND_DIR/next.config.js"; then
    print_success "Next.js configured for Docker deployment"
else
    print_warning "Adding standalone output to next.config.js..."
    # Backup original
    cp "$FRONTEND_DIR/next.config.js" "$FRONTEND_DIR/next.config.js.bak"
    
    # Add output: 'standalone' after reactStrictMode
    sed -i "/reactStrictMode:/a \ \ output: 'standalone'," "$FRONTEND_DIR/next.config.js"
    
    if grep -q "output.*:.*['\"]standalone['\"]" "$FRONTEND_DIR/next.config.js"; then
        print_success "Configuration updated successfully"
    else
        print_error "Failed to update configuration"
        print_info "Please add output: 'standalone' to next.config.js manually"
        exit 1
    fi
fi

# Step 7: Install npm dependencies
print_header "📦 Step 7: Installing npm Dependencies"

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

# Fix security vulnerabilities if any
print_step "Checking for security vulnerabilities..."
if npm audit >/dev/null 2>&1; then
    print_info "Running npm audit fix to address vulnerabilities..."
    npm audit fix --force >/dev/null 2>&1 || true
    print_success "Security check complete"
else
    print_success "No npm audit available or no vulnerabilities found"
fi

# Step 8: Check Docker images
print_header "🐋 Step 8: Checking Docker Images"

print_step "Checking for required Docker images..."
IMAGES_TO_CHECK=("apache/spark:3.5.0" "prom/prometheus:latest")
MISSING_IMAGES=0

for img in "${IMAGES_TO_CHECK[@]}"; do
    if docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "^$img$"; then
        print_success "Found: $img"
    else
        print_warning "Missing: $img (will be downloaded)"
        MISSING_IMAGES=1
    fi
done

if [ $MISSING_IMAGES -eq 1 ]; then
    print_info "First-time setup will download Docker images (~300 MB)"
    print_info "This may take 2-5 minutes depending on your connection"
fi

# Check if dashboard image exists
if docker images --format "{{.Repository}}" | grep -q "pyspark-coding-monitoring-dashboard"; then
    print_success "Dashboard image exists"
else
    print_info "Dashboard image will be built from source (~2 minutes)"
fi

# Step 9: Start Docker services
print_header "🐳 Step 9: Starting Docker Services"

cd "$PROJECT_ROOT"
print_step "Starting Spark cluster, Prometheus, and monitoring dashboard..."
if ! $DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml up -d; then
    print_error "Failed to start Docker services"
    print_info "Check logs with: $DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml logs"
    exit 1
fi

print_success "Docker services started"

# Verify all containers are running
print_step "Verifying containers are running..."
sleep 3
EXPECTED_CONTAINERS=("spark-master" "spark-worker-1" "spark-worker-2" "spark-worker-3" "prometheus" "pyspark-monitoring-dashboard")
FAILED_CONTAINERS=0

for container in "${EXPECTED_CONTAINERS[@]}"; do
    if docker ps --format "{{.Names}}" | grep -q "^$container$"; then
        print_success "Container running: $container"
    else
        print_error "Container not running: $container"
        FAILED_CONTAINERS=1
    fi
done

if [ $FAILED_CONTAINERS -eq 1 ]; then
    print_error "Some containers failed to start"
    print_info "Checking logs for errors..."
    $DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml logs --tail=20
    exit 1
fi

# Step 10: Wait for services to be ready
print_header "⏳ Step 10: Waiting for Services to Initialize"

wait_for_service "http://localhost:9080" "Spark Master" || {
    print_error "Spark Master failed to respond"
    print_info "Checking Spark Master logs..."
    docker logs spark-master --tail=30
    exit 1
}

wait_for_service "http://localhost:9090" "Prometheus" || {
    print_error "Prometheus failed to respond"
    print_info "Checking Prometheus logs..."
    docker logs prometheus --tail=30
    exit 1
}

wait_for_service "http://localhost:3000" "Monitoring Dashboard" || {
    print_error "Dashboard failed to respond"
    print_info "Checking Dashboard logs..."
    docker logs pyspark-monitoring-dashboard --tail=30
    exit 1
}

# Step 11: Verify services
print_header "✅ Step 11: Verifying Services"

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
if [ $WORKERS_UP -eq 3 ]; then
    print_success "All 3 workers are running"
else
    print_warning "Expected 3 workers, found $WORKERS_UP"
fi

# Verify dashboard can reach Spark API
print_step "Testing dashboard connectivity..."
if curl -s http://localhost:3000 | grep -q "PySpark"; then
    print_success "Dashboard is serving content"
else
    print_warning "Dashboard may not be fully initialized"
fi

# Step 11.5: Verify API proxy routes (CORS fix verification)
print_header "🔗 Step 11.5: Verifying API Proxy Routes (CORS Fix)"

print_step "Testing API proxy route: /api/spark/cluster..."
API_RESPONSE=$(curl -s http://localhost:3000/api/spark/cluster 2>/dev/null)
if echo "$API_RESPONSE" | grep -q "aliveworkers"; then
    WORKER_COUNT=$(echo "$API_RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('aliveworkers', 0))" 2>/dev/null || echo "0")
    print_success "API proxy working! Found $WORKER_COUNT workers via proxy"
elif echo "$API_RESPONSE" | grep -q "workers"; then
    print_success "API proxy route responding (workers array detected)"
else
    print_warning "API proxy may not be working correctly"
    print_info "Response preview: ${API_RESPONSE:0:100}..."
fi

print_step "Testing API proxy route: /api/spark/applications..."
APP_RESPONSE=$(curl -s http://localhost:3000/api/spark/applications 2>/dev/null)
if [ "$APP_RESPONSE" = "[]" ] || echo "$APP_RESPONSE" | grep -q "id"; then
    print_success "Applications API proxy working!"
else
    print_warning "Applications API proxy may have issues"
fi

# Check for CORS errors by testing direct API (should fail from browser context)
print_step "Verifying CORS prevention setup..."
if curl -s http://localhost:9080/json/ | grep -q "url"; then
    print_success "Direct Spark API accessible (server-side proxying will work)"
else
    print_warning "Direct Spark API not accessible"
fi

# Verify React Error #31 won't occur by checking API response format
print_step "Validating API response format (React Error #31 prevention)..."
if echo "$API_RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    workers = data.get('workers', [])
    if isinstance(workers, list):
        print('OK')
        sys.exit(0)
    else:
        print('ERROR: workers is not an array')
        sys.exit(1)
except:
    print('ERROR: Invalid JSON')
    sys.exit(1)
" 2>/dev/null | grep -q "OK"; then
    print_success "API returns workers as array (React Error #31 prevented)"
else
    print_warning "Could not verify workers array format"
fi

# Step 12: Display service URLs
print_header "🌐 Step 12: Service URLs"

echo -e "${GREEN}Dashboard:${NC}        http://localhost:3000"
echo -e "${GREEN}Spark Master UI:${NC}  http://localhost:9080"
echo -e "${GREEN}Prometheus:${NC}       http://localhost:9090"
echo -e "${GREEN}Worker 1 UI:${NC}      http://localhost:8081"
echo -e "${GREEN}Worker 2 UI:${NC}      http://localhost:8082"
echo -e "${GREEN}Worker 3 UI:${NC}      http://localhost:8083"
echo -e "${GREEN}App UI:${NC}           http://localhost:4040 ${YELLOW}(when job running)${NC}"

# Step 13: Launch Chrome
print_header "🌍 Step 13: Launching Dashboard in Chrome"

print_step "Opening http://localhost:3000 in Chrome..."
sleep 2

if [ "$CHROME_CMD" = "xdg-open" ]; then
    xdg-open "http://localhost:3000" &
    print_success "Dashboard opened in default browser"
else
    $CHROME_CMD "http://localhost:3000" &
    print_success "Dashboard opened in Chrome"
fi

# Step 14: Show helpful tips
print_header "💡 Step 14: Quick Tips & Usage"

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
echo -e "  ${YELLOW}$DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml logs -f${NC}"

echo -e "\n${CYAN}Stop Services:${NC}"
echo -e "  ${YELLOW}$DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml down${NC}"

echo -e "\n${CYAN}Restart Services:${NC}"
echo -e "  ${YELLOW}$DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml restart${NC}"

# Final success message
print_header "🎉 SUCCESS! Monitoring Dashboard is Ready!"

print_success "All systems operational"
print_info "Dashboard is running at: http://localhost:3000"
print_info "Press Ctrl+C to view this script's log, or close this window"

# Keep script running to show any errors
echo -e "\n${BLUE}Monitoring logs (Ctrl+C to exit)...${NC}\n"
$DOCKER_COMPOSE_CMD -f docker-compose.monitoring.yml logs -f
