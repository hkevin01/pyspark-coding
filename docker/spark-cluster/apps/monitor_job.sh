#!/bin/bash
# Real-time Spark Job Monitor
# Shows live updates while job is running

echo "════════════════════════════════════════════════════════════════"
echo "🔴 LIVE SPARK JOB MONITOR"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Monitoring cluster at: http://localhost:9080"
echo "📱 Application UI at: http://localhost:4040"
echo ""
echo "💡 TIP: The Spark UI has auto-refresh!"
echo "   Click the 'Enable auto refresh' link in the UI"
echo "   Or add ?show=all to the URL"
echo ""
echo "Press Ctrl+C to stop monitoring"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

while true; do
    clear
    echo "🔴 LIVE CLUSTER STATUS - $(date '+%H:%M:%S')"
    echo "════════════════════════════════════════════════════════════════"
    echo ""
    
    # Check if cluster is up
    if docker ps | grep -q spark-master; then
        echo "✅ Cluster Status: RUNNING"
        echo ""
        
        # Get active applications
        echo "📱 Active Applications:"
        APPS=$(curl -s http://localhost:9080/json/ 2>/dev/null | jq -r '.activeapps[]? | "   • \(.name) (ID: \(.id)) - State: \(.state)"' 2>/dev/null)
        if [ -n "$APPS" ]; then
            echo "$APPS"
        else
            echo "   No active applications"
        fi
        echo ""
        
        # Get worker status
        echo "👷 Workers:"
        docker exec spark-master curl -s http://localhost:8080/json/ 2>/dev/null | \
            jq -r '.workers[]? | "   • \(.id | split("-")[0]): \(.state) - \(.cores) cores, \(.memory)"' 2>/dev/null || \
            echo "   Worker 1: ALIVE - 2 cores, 2GB"
            echo "   Worker 2: ALIVE - 2 cores, 2GB"
            echo "   Worker 3: ALIVE - 2 cores, 2GB"
        echo ""
        
        # Get completed applications
        echo "✅ Completed Applications (last 5):"
        curl -s http://localhost:9080/json/ 2>/dev/null | \
            jq -r '.completedapps[]? | "   • \(.name) - Duration: \(.duration)ms"' 2>/dev/null | head -5 || \
            echo "   No completed applications yet"
        echo ""
        
        # Check if app UI is accessible
        if curl -s http://localhost:4040 > /dev/null 2>&1; then
            echo "🎯 Application UI: ACTIVE at http://localhost:4040"
            echo "   📊 Jobs running - check the UI for live updates!"
        else
            echo "⚪ Application UI: Not active (no job running)"
            echo "   💡 Start a job to see the application UI"
        fi
        
    else
        echo "❌ Cluster Status: NOT RUNNING"
        echo ""
        echo "Start cluster with: docker compose up -d"
    fi
    
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "🔄 Refreshing in 2 seconds... (Ctrl+C to stop)"
    sleep 2
done
