#!/usr/bin/env python3
"""
PySpark Monitoring Dashboard Pre-Launch Validation Script

This script performs comprehensive checks before launching the monitoring dashboard:
- CORS configuration validation
- React component property name validation
- TypeScript interface consistency
- API proxy route validation
- Docker configuration validation
- Environment variable checks
"""

import os
import re
import sys
import json
import subprocess
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# Colors for terminal output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    PURPLE = '\033[0;35m'
    CYAN = '\033[0;36m'
    NC = '\033[0m'  # No Color

def print_header(msg: str):
    print(f"\n{Colors.CYAN}{'═' * 64}{Colors.NC}")
    print(f"{Colors.CYAN}{msg}{Colors.NC}")
    print(f"{Colors.CYAN}{'═' * 64}{Colors.NC}\n")

def print_success(msg: str):
    print(f"{Colors.GREEN}✅ {msg}{Colors.NC}")

def print_error(msg: str):
    print(f"{Colors.RED}❌ {msg}{Colors.NC}")

def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠️  {msg}{Colors.NC}")

def print_info(msg: str):
    print(f"{Colors.BLUE}ℹ️  {msg}{Colors.NC}")

def print_step(msg: str):
    print(f"{Colors.PURPLE}▶ {msg}{Colors.NC}")

class ValidationResult:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.infos: List[str] = []
        self.passed: int = 0
        self.failed: int = 0
    
    def add_error(self, msg: str):
        self.errors.append(msg)
        self.failed += 1
        print_error(msg)
    
    def add_warning(self, msg: str):
        self.warnings.append(msg)
        print_warning(msg)
    
    def add_success(self, msg: str):
        self.passed += 1
        print_success(msg)
    
    def add_info(self, msg: str):
        self.infos.append(msg)
        print_info(msg)
    
    def is_valid(self) -> bool:
        return len(self.errors) == 0

# Get project paths - go up from scripts/ to monitoring_frontend/ to src/ to project root
SCRIPT_DIR = Path(__file__).parent.resolve()
FRONTEND_DIR = SCRIPT_DIR.parent  # monitoring_frontend/
SRC_DIR = FRONTEND_DIR.parent  # src/
PROJECT_ROOT = SRC_DIR.parent  # pyspark-coding/

def validate_api_proxy_routes(result: ValidationResult):
    """Check that API proxy routes exist and are properly configured."""
    print_step("Checking API proxy routes for CORS prevention...")
    
    api_dir = FRONTEND_DIR / "pages" / "api" / "spark"
    required_routes = ["cluster.ts", "applications.ts", "jobs.ts", "stages.ts", "executors.ts", "environment.ts"]
    
    if not api_dir.exists():
        result.add_error(f"API proxy directory not found: {api_dir}")
        return
    
    for route in required_routes:
        route_path = api_dir / route
        if route_path.exists():
            # Check route content for proper proxy pattern
            content = route_path.read_text()
            
            # Check for internal URL usage (CORS fix pattern)
            if "SPARK_MASTER_INTERNAL_URL" in content or "SPARK_APP_INTERNAL_URL" in content or "spark-master" in content:
                result.add_success(f"API route {route} uses internal Docker network (CORS safe)")
            elif "localhost:9080" in content or "localhost:4040" in content:
                result.add_error(f"API route {route} uses localhost URLs - will cause CORS issues in Docker!")
            else:
                result.add_warning(f"API route {route} - could not verify CORS configuration")
        else:
            result.add_error(f"Missing API proxy route: {route}")

def validate_typescript_interfaces(result: ValidationResult):
    """Check TypeScript interfaces match Spark API response format."""
    print_step("Validating TypeScript interface property names...")
    
    spark_api_path = FRONTEND_DIR / "lib" / "sparkApi.ts"
    
    if not spark_api_path.exists():
        result.add_error(f"sparkApi.ts not found: {spark_api_path}")
        return
    
    content = spark_api_path.read_text()
    
    # Check for correct lowercase property names (Spark API uses lowercase)
    correct_props = {
        "ClusterStatus": ["aliveworkers", "coresused", "memoryused"],
        "SparkWorker": ["coresused", "coresfree", "memoryused", "memoryfree", "webuiaddress", "lastheartbeat"]
    }
    
    # Check for incorrect camelCase property names that would cause React Error #31
    incorrect_props = {
        "ClusterStatus": ["coresUsed", "memoryUsed"],
        "SparkWorker": ["coresUsed", "coresFree", "memoryUsed", "memoryFree"]
    }
    
    for interface, props in correct_props.items():
        for prop in props:
            # Use word boundary to match exact property name
            pattern = rf'\b{prop}\b.*:'
            if re.search(pattern, content):
                result.add_success(f"{interface}.{prop} uses correct lowercase format")
            else:
                result.add_warning(f"{interface}.{prop} not found - may need verification")
    
    # Check for incorrect camelCase that would break
    for interface, props in incorrect_props.items():
        for prop in props:
            pattern = rf'\b{prop}\b\s*:'
            if re.search(pattern, content):
                result.add_error(f"{interface}.{prop} uses camelCase - Spark API returns lowercase! (React Error #31)")

def validate_component_property_usage(result: ValidationResult):
    """Check React components use correct property names."""
    print_step("Validating React component property usage...")
    
    components_dir = FRONTEND_DIR / "components"
    
    if not components_dir.exists():
        result.add_error(f"Components directory not found: {components_dir}")
        return
    
    incorrect_patterns = {
        "cluster.coresUsed": r"cluster\.coresUsed",
        "cluster.memoryUsed": r"cluster\.memoryUsed",
        "worker.coresUsed": r"worker\.coresUsed",
        "worker.memoryUsed": r"worker\.memoryUsed",
    }
    
    for tsx_file in components_dir.glob("*.tsx"):
        content = tsx_file.read_text()
        filename = tsx_file.name
        
        # Check for incorrect camelCase usage
        for prop_name, pattern in incorrect_patterns.items():
            if re.search(pattern, content):
                result.add_error(f"{filename}: Uses {prop_name} (camelCase) - should be lowercase!")
        
        # Verify correct patterns exist where expected
        if "ClusterOverview" in filename:
            if re.search(r"cluster\.aliveworkers", content):
                result.add_success(f"{filename}: Correctly uses cluster.aliveworkers")
            if re.search(r"cluster\.coresused", content):
                result.add_success(f"{filename}: Correctly uses cluster.coresused")
        
        if "WorkersList" in filename:
            if re.search(r"worker\.coresused", content):
                result.add_success(f"{filename}: Correctly uses worker.coresused")
        
        if "MetricsChart" in filename:
            if re.search(r"cluster\.coresused", content):
                result.add_success(f"{filename}: Correctly uses cluster.coresused")

def validate_next_config(result: ValidationResult):
    """Check Next.js configuration for Docker deployment."""
    print_step("Validating Next.js configuration...")
    
    config_path = FRONTEND_DIR / "next.config.js"
    
    if not config_path.exists():
        result.add_error(f"next.config.js not found: {config_path}")
        return
    
    content = config_path.read_text()
    
    # Check for standalone output (required for Docker)
    if re.search(r"output\s*:\s*['\"]standalone['\"]", content):
        result.add_success("Next.js configured with output: 'standalone' for Docker")
    else:
        result.add_error("Missing output: 'standalone' in next.config.js - Docker build will fail!")

def validate_docker_compose(result: ValidationResult):
    """Check Docker Compose configuration."""
    print_step("Validating Docker Compose configuration...")
    
    compose_path = PROJECT_ROOT / "docker-compose.monitoring.yml"
    
    if not compose_path.exists():
        result.add_error(f"docker-compose.monitoring.yml not found: {compose_path}")
        result.add_info(f"Expected at: {compose_path}")
        return
    
    content = compose_path.read_text()
    
    # Check for internal URL environment variables (for CORS proxy)
    internal_urls = [
        "SPARK_MASTER_INTERNAL_URL",
        "SPARK_APP_INTERNAL_URL",
        "PROMETHEUS_INTERNAL_URL"
    ]
    
    for url_var in internal_urls:
        if url_var in content:
            result.add_success(f"Docker Compose has {url_var} for internal networking")
        else:
            result.add_warning(f"Missing {url_var} in Docker Compose - API proxy may not work")
    
    # Check for required services
    required_services = ["spark-master", "spark-worker", "prometheus", "monitoring-dashboard"]
    for service in required_services:
        if service in content:
            result.add_success(f"Docker Compose includes {service} service")
        else:
            result.add_error(f"Missing {service} service in Docker Compose")
    
    # Check for Docker network
    if "spark-network" in content or "networks:" in content:
        result.add_success("Docker Compose defines custom network for inter-service communication")
    else:
        result.add_warning("No custom Docker network found - services may have connectivity issues")

def validate_package_json(result: ValidationResult):
    """Check package.json for required dependencies."""
    print_step("Validating package.json dependencies...")
    
    package_path = FRONTEND_DIR / "package.json"
    
    if not package_path.exists():
        result.add_error(f"package.json not found: {package_path}")
        return
    
    try:
        with open(package_path) as f:
            package = json.load(f)
    except json.JSONDecodeError as e:
        result.add_error(f"Invalid JSON in package.json: {e}")
        return
    
    deps = package.get("dependencies", {})
    dev_deps = package.get("devDependencies", {})
    all_deps = {**deps, **dev_deps}
    
    required_deps = ["next", "react", "react-dom", "axios", "swr", "recharts"]
    
    for dep in required_deps:
        if dep in all_deps:
            result.add_success(f"Dependency {dep} is installed ({all_deps[dep]})")
        else:
            result.add_error(f"Missing required dependency: {dep}")

def validate_env_configuration(result: ValidationResult):
    """Check environment variable configuration."""
    print_step("Validating environment configuration...")
    
    env_example = FRONTEND_DIR / ".env.example"
    env_local = FRONTEND_DIR / ".env.local"
    
    if env_example.exists():
        result.add_success(".env.example exists for documentation")
    else:
        result.add_warning("No .env.example file - consider adding for documentation")
    
    # Check if .env.local exists (optional)
    if env_local.exists():
        result.add_success(".env.local exists for local development")
    else:
        result.add_info(".env.local not found - using Docker environment variables")

def validate_dockerfile(result: ValidationResult):
    """Check Dockerfile configuration."""
    print_step("Validating Dockerfile...")
    
    dockerfile_path = FRONTEND_DIR / "Dockerfile"
    
    if not dockerfile_path.exists():
        result.add_error(f"Dockerfile not found: {dockerfile_path}")
        return
    
    content = dockerfile_path.read_text()
    
    # Check for multi-stage build
    if "FROM" in content and content.count("FROM") >= 2:
        result.add_success("Dockerfile uses multi-stage build for optimization")
    else:
        result.add_warning("Dockerfile may not use multi-stage build")
    
    # Check for standalone copy
    if "standalone" in content:
        result.add_success("Dockerfile copies standalone build output")
    else:
        result.add_warning("Dockerfile may not be configured for standalone output")
    
    # Check for proper node image
    if "node:18" in content or "node:20" in content:
        result.add_success("Dockerfile uses modern Node.js version")
    elif "node:" in content:
        result.add_warning("Verify Node.js version is compatible (18+ recommended)")

def check_direct_api_calls(result: ValidationResult):
    """Check for any direct API calls that would cause CORS issues."""
    print_step("Checking for direct API calls (CORS risk)...")
    
    lib_dir = FRONTEND_DIR / "lib"
    components_dir = FRONTEND_DIR / "components"
    
    # These patterns indicate actual API calls that would cause CORS issues
    dangerous_api_patterns = [
        (r"axios\.get\(['\"]http://localhost:9080", "Direct axios call to Spark Master"),
        (r"axios\.get\(['\"]http://localhost:4040", "Direct axios call to Spark App UI"),
        (r"axios\.get\(['\"]http://localhost:9090", "Direct axios call to Prometheus"),
        (r"fetch\(['\"]http://localhost:9080", "Direct fetch call to Spark Master"),
        (r"fetch\(['\"]http://localhost:4040", "Direct fetch call to Spark App UI"),
        (r"fetch\(['\"]http://localhost:9090", "Direct fetch call to Prometheus"),
    ]
    
    # These are just links in the UI, not API calls - they're OK
    link_patterns = [
        r"href=['\"]http://localhost:",  # UI links are fine
        r"target=['\"]_blank['\"]",  # External link target
    ]
    
    safe_indicator = "/api/spark/"
    
    dirs_to_check = [lib_dir, components_dir]
    found_issues = False
    
    for dir_path in dirs_to_check:
        if not dir_path.exists():
            continue
        
        for ts_file in dir_path.glob("*.ts*"):
            content = ts_file.read_text()
            filename = ts_file.name
            
            # If file uses proxy routes, it's safe
            if safe_indicator in content:
                continue
            
            for pattern, description in dangerous_api_patterns:
                if re.search(pattern, content):
                    result.add_warning(f"{filename}: Found {description} - may cause CORS issues")
                    found_issues = True
    
    if not found_issues:
        result.add_success("No direct API calls found that could cause CORS issues")

def validate_workers_array_type(result: ValidationResult):
    """Check that ClusterStatus.workers is typed as SparkWorker[] not number."""
    print_step("Validating workers type (must be array, not number)...")
    
    spark_api_path = FRONTEND_DIR / "lib" / "sparkApi.ts"
    
    if not spark_api_path.exists():
        return  # Already checked elsewhere
    
    content = spark_api_path.read_text()
    
    # Check for correct workers type
    if re.search(r"workers\s*:\s*SparkWorker\[\]", content):
        result.add_success("ClusterStatus.workers correctly typed as SparkWorker[]")
    elif re.search(r"workers\s*:\s*number", content):
        result.add_error("ClusterStatus.workers typed as 'number' - should be 'SparkWorker[]' (React Error #31)")
    else:
        result.add_warning("Could not verify workers type in ClusterStatus interface")

def print_summary(result: ValidationResult):
    """Print validation summary."""
    print_header("📊 Validation Summary")
    
    total = result.passed + result.failed
    
    print(f"  {Colors.GREEN}Passed:{Colors.NC} {result.passed}")
    print(f"  {Colors.RED}Failed:{Colors.NC} {result.failed}")
    print(f"  {Colors.YELLOW}Warnings:{Colors.NC} {len(result.warnings)}")
    print(f"  {Colors.BLUE}Total Checks:{Colors.NC} {total}")
    
    if result.is_valid():
        print(f"\n{Colors.GREEN}{'═' * 64}{Colors.NC}")
        print(f"{Colors.GREEN}✅ ALL CRITICAL CHECKS PASSED - Safe to launch!{Colors.NC}")
        print(f"{Colors.GREEN}{'═' * 64}{Colors.NC}")
        return 0
    else:
        print(f"\n{Colors.RED}{'═' * 64}{Colors.NC}")
        print(f"{Colors.RED}❌ VALIDATION FAILED - Fix errors before launching!{Colors.NC}")
        print(f"{Colors.RED}{'═' * 64}{Colors.NC}")
        print(f"\n{Colors.RED}Errors to fix:{Colors.NC}")
        for error in result.errors:
            print(f"  • {error}")
        return 1

def main():
    print_header("🔍 PySpark Monitoring Dashboard Validation")
    
    # Print detected paths for debugging
    print_info(f"Frontend directory: {FRONTEND_DIR}")
    print_info(f"Project root: {PROJECT_ROOT}")
    print()
    
    result = ValidationResult()
    
    # Run all validations
    validate_api_proxy_routes(result)
    print()
    
    validate_typescript_interfaces(result)
    print()
    
    validate_workers_array_type(result)
    print()
    
    validate_component_property_usage(result)
    print()
    
    validate_next_config(result)
    print()
    
    validate_docker_compose(result)
    print()
    
    validate_package_json(result)
    print()
    
    validate_env_configuration(result)
    print()
    
    validate_dockerfile(result)
    print()
    
    check_direct_api_calls(result)
    print()
    
    # Print summary and exit with appropriate code
    exit_code = print_summary(result)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
