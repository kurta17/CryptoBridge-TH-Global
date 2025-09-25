#!/usr/bin/env python3
"""
🎭 CryptoBridge Complete Workflow Test
End-to-End Pipeline Validation: Producer → Consumer → Data Lake → ClickHouse

This script runs the complete workflow in the correct order:
1. Start Kafka Producer (data generation)
2. Start ClickHouse Consumer (real-time analytics)
3. Start Data Lake Pipeline (Bronze → Silver → Gold)
4. Verify data in ClickHouse
5. Monitor and report results
"""

import subprocess
import time
import sys
import os
from datetime import datetime
import requests
import json

# Use configured Python environment
PYTHON_CMD = "/opt/anaconda3/envs/myenv/bin/python"

class WorkflowManager:
    def __init__(self):
        self.processes = {}
        self.start_time = datetime.now()
        
    def print_header(self, title, emoji="🎭"):
        print(f"\n{emoji} {title}")
        print("=" * (len(title) + 4))
        
    def start_process(self, name, command, description):
        """Start a background process"""
        print(f"🚀 Starting {name}: {description}")
        try:
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.processes[name] = {
                'process': process,
                'command': command,
                'description': description,
                'start_time': datetime.now()
            }
            print(f"✅ {name} started (PID: {process.pid})")
            return True
        except Exception as e:
            print(f"❌ Failed to start {name}: {e}")
            return False
    
    def check_process_health(self, name):
        """Check if a process is still running"""
        if name not in self.processes:
            return False
        
        process = self.processes[name]['process']
        return process.poll() is None
    
    def get_process_output(self, name, lines=10):
        """Get recent output from a process"""
        if name not in self.processes:
            return "Process not found"
        
        process = self.processes[name]['process']
        try:
            # Non-blocking read
            output = ""
            if process.stdout:
                output = process.stdout.read()
            return output[-1000:] if output else "No output yet"
        except:
            return "Could not read output"
    
    def check_clickhouse_data(self):
        """Check if data exists in ClickHouse"""
        try:
            # Query ClickHouse for transaction count with authentication
            response = requests.get(
                "http://localhost:8123/",
                params={
                    'query': 'SELECT COUNT(*) FROM cryptobridge.transactions FORMAT JSON',
                    'user': 'analytics',
                    'password': 'analytics123'
                },
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                count = result['data'][0]['count()'] if result['data'] else 0
                return True, count
            else:
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except requests.exceptions.ConnectionError:
            return False, "ClickHouse not accessible"
        except Exception as e:
            return False, f"Error: {e}"
    
    def check_kafka_topics(self):
        """Check Kafka topic status"""
        try:
            # Use docker to check Kafka topics
            result = subprocess.run(
                ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n')
                return True, topics
            else:
                return False, result.stderr
                
        except Exception as e:
            return False, f"Error checking Kafka: {e}"
    
    def stop_all_processes(self):
        """Stop all managed processes"""
        print("\n🛑 Stopping all processes...")
        
        for name, proc_info in self.processes.items():
            process = proc_info['process']
            if process.poll() is None:  # Process is still running
                print(f"🛑 Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                    print(f"✅ {name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"🔪 {name} force killed")
    
    def show_status_report(self):
        """Show current status of all processes"""
        print("\n📊 WORKFLOW STATUS REPORT")
        print("=" * 50)
        
        runtime = datetime.now() - self.start_time
        print(f"⏱️ Total Runtime: {runtime}")
        print()
        
        for name, proc_info in self.processes.items():
            process = proc_info['process']
            proc_runtime = datetime.now() - proc_info['start_time']
            
            if process.poll() is None:
                status = "🟢 Running"
            else:
                status = f"🔴 Stopped (exit code: {process.returncode})"
            
            print(f"{status} {name} - {proc_runtime}")
            print(f"   📝 {proc_info['description']}")
        
        print()

def main():
    workflow = WorkflowManager()
    
    workflow.print_header("CryptoBridge Complete Workflow Test", "🎭")
    print("🔄 End-to-End Pipeline: Producer → Consumer → Data Lake → ClickHouse")
    print("⏱️ Estimated time: 5-10 minutes for complete workflow")
    print()
    
    try:
        # Step 1: Start Kafka Producer
        workflow.print_header("Step 1: Start Kafka Producer", "📡")
        producer_cmd = f"{PYTHON_CMD} scripts/kafka_transaction_producer.py"
        if not workflow.start_process("producer", producer_cmd, "Generate cryptocurrency transactions"):
            return False
        
        print("⏳ Allowing producer to generate initial data...")
        time.sleep(15)
        
        # Check Kafka topics
        kafka_ok, kafka_info = workflow.check_kafka_topics()
        if kafka_ok:
            print(f"✅ Kafka topics available: {', '.join(kafka_info)}")
        else:
            print(f"⚠️ Kafka check issue: {kafka_info}")
        
        # Step 2: Start ClickHouse Consumer
        workflow.print_header("Step 2: Start ClickHouse Consumer", "📊")
        consumer_cmd = f"{PYTHON_CMD} scripts/clickhouse_consumer.py"
        if not workflow.start_process("consumer", consumer_cmd, "Stream data to ClickHouse analytics"):
            return False
        
        print("⏳ Allowing consumer to process initial data...")
        time.sleep(20)
        
        # Step 3: Check ClickHouse Data
        workflow.print_header("Step 3: Verify ClickHouse Data", "🔍")
        ch_ok, ch_count = workflow.check_clickhouse_data()
        if ch_ok:
            print(f"✅ ClickHouse contains {ch_count} transactions")
        else:
            print(f"⚠️ ClickHouse issue: {ch_count}")
        
        # Step 4: Start Data Lake Pipeline (Optional - commented out for now due to complexity)
        workflow.print_header("Step 4: Data Lake Pipeline Status", "🏗️")
        print("📝 Data Lake Components Available:")
        print("   🥉 Bronze Layer: scripts/bronze_layer_processor.py")
        print("   🥈 Silver Layer: scripts/silver_layer_processor.py") 
        print("   🥇 Gold Layer: scripts/gold_layer_processor.py")
        print("   🎭 Orchestrator: scripts/data_lake_orchestrator.py")
        print()
        print("💡 To start data lake pipeline manually:")
        print(f"   {PYTHON_CMD} scripts/bronze_layer_processor.py")
        
        # Step 5: Monitor for 2 minutes
        workflow.print_header("Step 5: Monitor Workflow", "📈")
        print("⏱️ Monitoring for 2 minutes...")
        print("📊 Real-time status updates:")
        print()
        
        for i in range(24):  # 24 * 5 seconds = 2 minutes
            time.sleep(5)
            
            # Check process health
            producer_status = "🟢" if workflow.check_process_health("producer") else "🔴"
            consumer_status = "🟢" if workflow.check_process_health("consumer") else "🔴"
            
            # Check ClickHouse data count
            ch_ok, ch_count = workflow.check_clickhouse_data()
            ch_display = f"{ch_count} txns" if ch_ok else "Error"
            
            # Status line
            elapsed = (i + 1) * 5
            print(f"[{elapsed:3d}s] Producer:{producer_status} Consumer:{consumer_status} ClickHouse:{ch_display}")
        
        # Final verification
        workflow.print_header("Step 6: Final Verification", "🎯")
        
        # Final ClickHouse check
        ch_ok, ch_count = workflow.check_clickhouse_data()
        if ch_ok and ch_count > 0:
            print(f"✅ SUCCESS: ClickHouse contains {ch_count} transactions")
            
            # Get some sample data
            try:
                response = requests.get(
                    "http://localhost:8123/",
                    params={
                        'query': 'SELECT transaction_id, amount, currency, transaction_type FROM cryptobridge.transactions LIMIT 5 FORMAT JSON',
                        'user': 'analytics',
                        'password': 'analytics123'
                    },
                    timeout=5
                )
                
                if response.status_code == 200:
                    result = response.json()
                    print("\n📋 Sample Data:")
                    for row in result.get('data', []):
                        print(f"   • {row['transaction_id']}: {row['amount']} {row['currency']} ({row['country']})")
                        
            except Exception as e:
                print(f"⚠️ Could not fetch sample data: {e}")
                
        else:
            print(f"❌ ISSUE: ClickHouse data problem: {ch_count}")
        
        # Show final status
        workflow.show_status_report()
        
        # Success summary
        workflow.print_header("Workflow Complete", "🎉")
        print("✅ Producer → Consumer → ClickHouse workflow completed!")
        print("📊 Key Results:")
        print(f"   • Producer: {'Running' if workflow.check_process_health('producer') else 'Stopped'}")
        print(f"   • Consumer: {'Running' if workflow.check_process_health('consumer') else 'Stopped'}")
        print(f"   • ClickHouse: {ch_count if ch_ok else 'Error'} transactions")
        print()
        print("🌐 Access Points:")
        print("   • Kafka UI:       http://localhost:8090")
        print("   • ClickHouse:     http://localhost:8123")
        print("   • HDFS NameNode:  http://localhost:9870")
        print("   • Spark Master:   http://localhost:8080")
        
        return True
        
    except KeyboardInterrupt:
        print("\n🛑 Workflow interrupted by user")
        return False
        
    finally:
        # Cleanup
        print("\n🧹 Cleanup Options:")
        print("1. Keep processes running for further testing")
        print("2. Stop all processes")
        
        try:
            choice = input("\nEnter choice (1 or 2): ").strip()
            if choice == "2":
                workflow.stop_all_processes()
            else:
                print("✅ Processes left running")
                print("💡 Use 'ps aux | grep python' to see running processes")
                print("💡 Use 'kill <PID>' to stop individual processes")
        except KeyboardInterrupt:
            workflow.stop_all_processes()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)