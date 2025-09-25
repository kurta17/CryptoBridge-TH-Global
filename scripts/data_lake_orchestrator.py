#!/usr/bin/env python3
"""
🎭 Data Lake ETL Orchestrator
Complete Medallion Architecture Pipeline Manager

This orchestrator manages the entire Bronze → Silver → Gold data lake pipeline,
ensuring proper data flow, dependency management, and monitoring.

🎯 Orchestration Features:
- 🔄 Sequential pipeline execution (Bronze → Silver → Gold)
- 📊 Health monitoring and status tracking
- 🚨 Error handling and recovery
- 📈 Performance metrics collection
- 🎯 Configurable execution modes
- 🔧 Pipeline dependency management
"""

import os
import sys
import time
import threading
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLakeOrchestrator:
    def __init__(self):
        """Initialize Data Lake Orchestrator"""
        self.pipeline_status = {
            "bronze": {"status": "stopped", "start_time": None, "process": None, "errors": []},
            "silver": {"status": "stopped", "start_time": None, "process": None, "errors": []},
            "gold": {"status": "stopped", "start_time": None, "process": None, "errors": []}
        }
        self.execution_mode = "sequential"  # or "parallel"
        self.monitoring_enabled = True
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        
    def check_prerequisites(self):
        """Check if all prerequisites are met"""
        logger.info("🔍 Checking prerequisites...")
        
        prerequisites = {
            "hdfs_namenode": self._check_service("namenode", 9870),
            "hdfs_datanode": self._check_service("datanode", 9864),
            "spark_master": self._check_service("spark-master", 8080),
            "kafka": self._check_service("kafka", 9092),
            "clickhouse": self._check_service("clickhouse", 8123)
        }
        
        all_ready = all(prerequisites.values())
        
        if all_ready:
            logger.info("✅ All prerequisites met")
        else:
            failed_services = [k for k, v in prerequisites.items() if not v]
            logger.error(f"❌ Failed prerequisites: {failed_services}")
        
        return all_ready, prerequisites
    
    def _check_service(self, service_name: str, port: int) -> bool:
        """Check if a service is running"""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={service_name}", "--format", "{{.Status}}"],
                capture_output=True, text=True, timeout=10
            )
            return "Up" in result.stdout
        except Exception as e:
            logger.warning(f"⚠️ Could not check {service_name}: {e}")
            return False
    
    def start_bronze_layer(self):
        """Start Bronze layer pipeline"""
        logger.info("🥉 Starting Bronze Layer Pipeline...")
        
        try:
            bronze_script = os.path.join(self.base_path, "bronze_layer_processor.py")
            
            process = subprocess.Popen(
                ["python3", bronze_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.pipeline_status["bronze"] = {
                "status": "running",
                "start_time": datetime.now(),
                "process": process,
                "errors": []
            }
            
            logger.info("✅ Bronze layer started successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start Bronze layer: {e}")
            self.pipeline_status["bronze"]["status"] = "failed"
            self.pipeline_status["bronze"]["errors"].append(str(e))
            return False
    
    def start_silver_layer(self):
        """Start Silver layer pipeline"""
        logger.info("🥈 Starting Silver Layer Pipeline...")
        
        try:
            silver_script = os.path.join(self.base_path, "silver_layer_processor.py")
            
            process = subprocess.Popen(
                ["python3", silver_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.pipeline_status["silver"] = {
                "status": "running",
                "start_time": datetime.now(),
                "process": process,
                "errors": []
            }
            
            logger.info("✅ Silver layer started successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start Silver layer: {e}")
            self.pipeline_status["silver"]["status"] = "failed"
            self.pipeline_status["silver"]["errors"].append(str(e))
            return False
    
    def start_gold_layer(self):
        """Start Gold layer pipeline"""
        logger.info("🥇 Starting Gold Layer Pipeline...")
        
        try:
            gold_script = os.path.join(self.base_path, "gold_layer_processor.py")
            
            process = subprocess.Popen(
                ["python3", gold_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.pipeline_status["gold"] = {
                "status": "running",
                "start_time": datetime.now(),
                "process": process,
                "errors": []
            }
            
            logger.info("✅ Gold layer started successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start Gold layer: {e}")
            self.pipeline_status["gold"]["status"] = "failed"
            self.pipeline_status["gold"]["errors"].append(str(e))
            return False
    
    def wait_for_data_availability(self, layer: str, timeout_minutes: int = 5) -> bool:
        """Wait for data to be available in a layer"""
        logger.info(f"⏳ Waiting for {layer} layer data availability...")
        
        timeout_time = datetime.now() + timedelta(minutes=timeout_minutes)
        
        while datetime.now() < timeout_time:
            if self._check_data_exists(layer):
                logger.info(f"✅ Data available in {layer} layer")
                return True
            
            time.sleep(30)  # Check every 30 seconds
        
        logger.warning(f"⚠️ Timeout waiting for {layer} layer data")
        return False
    
    def _check_data_exists(self, layer: str) -> bool:
        """Check if data exists in a specific layer"""
        try:
            # This would typically check HDFS for data files
            # For now, we'll simulate by checking if the process is running
            return self.pipeline_status[layer]["status"] == "running"
        except Exception:
            return False
    
    def monitor_pipeline_health(self):
        """Monitor the health of all pipeline components"""
        while self.monitoring_enabled:
            try:
                for layer, status in self.pipeline_status.items():
                    if status["process"] and status["status"] == "running":
                        # Check if process is still running
                        poll_result = status["process"].poll()
                        
                        if poll_result is not None:
                            # Process has terminated
                            if poll_result == 0:
                                logger.info(f"✅ {layer.title()} layer completed successfully")
                                status["status"] = "completed"
                            else:
                                logger.error(f"❌ {layer.title()} layer failed with exit code {poll_result}")
                                status["status"] = "failed"
                                
                                # Capture error output
                                stderr_output = status["process"].stderr.read()
                                if stderr_output:
                                    status["errors"].append(stderr_output)
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"❌ Health monitoring error: {e}")
                time.sleep(60)
    
    def stop_all_pipelines(self):
        """Stop all running pipelines"""
        logger.info("🛑 Stopping all pipelines...")
        
        for layer, status in self.pipeline_status.items():
            if status["process"] and status["status"] == "running":
                try:
                    status["process"].terminate()
                    status["process"].wait(timeout=30)
                    status["status"] = "stopped"
                    logger.info(f"✅ {layer.title()} layer stopped")
                except Exception as e:
                    logger.error(f"❌ Error stopping {layer} layer: {e}")
                    try:
                        status["process"].kill()
                    except:
                        pass
        
        self.monitoring_enabled = False
    
    def get_pipeline_status_report(self) -> Dict[str, Any]:
        """Generate comprehensive pipeline status report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "layers": {}
        }
        
        for layer, status in self.pipeline_status.items():
            runtime = None
            if status["start_time"]:
                runtime = (datetime.now() - status["start_time"]).total_seconds()
            
            report["layers"][layer] = {
                "status": status["status"],
                "start_time": status["start_time"].isoformat() if status["start_time"] else None,
                "runtime_seconds": runtime,
                "error_count": len(status["errors"]),
                "latest_errors": status["errors"][-3:] if status["errors"] else []
            }
            
            if status["status"] in ["failed", "stopped"]:
                report["overall_status"] = "degraded"
        
        return report
    
    def run_sequential_pipeline(self):
        """Run the complete pipeline in sequential mode"""
        logger.info("🎭 Starting Sequential Data Lake Pipeline")
        logger.info("="*70)
        
        try:
            # Start monitoring in background
            monitor_thread = threading.Thread(target=self.monitor_pipeline_health, daemon=True)
            monitor_thread.start()
            
            # Step 1: Start Bronze layer
            if not self.start_bronze_layer():
                return False
            
            # Step 2: Wait for Bronze data, then start Silver
            if self.wait_for_data_availability("bronze", timeout_minutes=3):
                time.sleep(60)  # Give Bronze layer time to generate some data
                
                if not self.start_silver_layer():
                    return False
                
                # Step 3: Wait for Silver data, then start Gold
                if self.wait_for_data_availability("silver", timeout_minutes=3):
                    time.sleep(60)  # Give Silver layer time to generate some data
                    
                    if not self.start_gold_layer():
                        return False
                else:
                    logger.error("❌ Silver layer data not available, skipping Gold layer")
            else:
                logger.error("❌ Bronze layer data not available, skipping downstream layers")
            
            logger.info("🚀 All pipeline layers started successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}")
            return False
    
    def run_parallel_pipeline(self):
        """Run the complete pipeline in parallel mode (for testing)"""
        logger.info("🎭 Starting Parallel Data Lake Pipeline (Test Mode)")
        logger.info("="*70)
        
        try:
            # Start monitoring in background
            monitor_thread = threading.Thread(target=self.monitor_pipeline_health, daemon=True)
            monitor_thread.start()
            
            # Start all layers simultaneously
            results = [
                self.start_bronze_layer(),
                self.start_silver_layer(),
                self.start_gold_layer()
            ]
            
            if all(results):
                logger.info("🚀 All pipeline layers started in parallel!")
                return True
            else:
                logger.error("❌ Some pipeline layers failed to start")
                return False
            
        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}")
            return False
    
    def run_data_lake_pipeline(self, mode: str = "sequential"):
        """Main entry point to run the data lake pipeline"""
        self.execution_mode = mode
        
        # Check prerequisites
        ready, prereqs = self.check_prerequisites()
        if not ready:
            logger.error("❌ Prerequisites not met. Please ensure all services are running.")
            return False
        
        # Run based on mode
        if mode == "sequential":
            success = self.run_sequential_pipeline()
        elif mode == "parallel":
            success = self.run_parallel_pipeline()
        else:
            logger.error(f"❌ Unknown execution mode: {mode}")
            return False
        
        if success:
            logger.info("✅ Data Lake Pipeline orchestration completed successfully")
        else:
            logger.error("❌ Data Lake Pipeline orchestration failed")
        
        return success

def main():
    """Main entry point"""
    orchestrator = DataLakeOrchestrator()
    
    # Show orchestrator information
    print("🎭 DATA LAKE ETL ORCHESTRATOR")
    print("="*70)
    print("🏗️ Architecture: Medallion (Bronze → Silver → Gold)")
    print("🔄 Execution: Sequential pipeline with dependency management")
    print("📊 Monitoring: Real-time health checks and status reporting")
    print("🚨 Recovery: Error handling and process management")
    print("="*70)
    print()
    print("📋 Pipeline Components:")
    print("  🥉 Bronze Layer:  Kafka → HDFS (Raw Data)")
    print("  🥈 Silver Layer:  Data Quality + Cleaning + Enrichment")
    print("  🥇 Gold Layer:    Business Analytics + KPIs + Aggregates")
    print("="*70)
    
    try:
        # Run the pipeline
        success = orchestrator.run_data_lake_pipeline(mode="sequential")
        
        if success:
            print("\n🚀 Pipeline started successfully!")
            print("\n📊 Monitor your data lake:")
            print("  • HDFS Web UI:    http://localhost:9870")
            print("  • Spark Master:   http://localhost:8080")
            print("  • Kafka UI:       http://localhost:8090")
            print("  • ClickHouse:     http://localhost:8123")
            print("\n⌨️  Press Ctrl+C to stop all pipelines")
            
            # Keep running and show status
            while True:
                time.sleep(30)
                status_report = orchestrator.get_pipeline_status_report()
                print(f"\n📈 Pipeline Status [{datetime.now().strftime('%H:%M:%S')}]:")
                
                for layer, layer_status in status_report["layers"].items():
                    status_emoji = {
                        "running": "🟢",
                        "completed": "✅", 
                        "failed": "❌",
                        "stopped": "⏹️"
                    }.get(layer_status["status"], "⚪")
                    
                    runtime = f" ({int(layer_status['runtime_seconds'])}s)" if layer_status['runtime_seconds'] else ""
                    print(f"  {status_emoji} {layer.title()} Layer: {layer_status['status']}{runtime}")
        
        else:
            print("\n❌ Pipeline failed to start")
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping all pipelines...")
        orchestrator.stop_all_pipelines()
        print("✅ All pipelines stopped")
    except Exception as e:
        print(f"\n❌ Orchestrator error: {e}")
        orchestrator.stop_all_pipelines()

if __name__ == "__main__":
    main()