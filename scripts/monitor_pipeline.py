#!/usr/bin/env python3
"""
📊 CryptoBridge Pipeline Monitor
Real-time monitoring for Kafka streaming pipeline

This script provides real-time monitoring and metrics for the streaming pipeline.
"""

import time
import json
import signal
import sys
from datetime import datetime
from typing import Dict, Any

try:
    from kafka import KafkaConsumer, KafkaAdminClient
    from kafka.structs import TopicPartition
except ImportError:
    print("❌ Missing kafka-python dependency!")
    print("Please install: pip install kafka-python")
    exit(1)

class PipelineMonitor:
    """📊 Real-time pipeline monitoring"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """🛑 Handle shutdown signals"""
        print("\n🛑 Shutting down monitor...")
        self.running = False
        sys.exit(0)
    
    def get_topic_metrics(self) -> Dict[str, Any]:
        """📈 Get topic-level metrics"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            
            # Get topic metadata
            metadata = admin_client.describe_topics()
            topics = list(metadata.topics.keys())
            
            # Filter CryptoBridge topics
            crypto_topics = [t for t in topics if t.startswith('transactions-') or t.startswith('analytics-')]
            
            metrics = {}
            for topic in crypto_topics:
                topic_info = metadata.topics[topic]
                metrics[topic] = {
                    'partitions': len(topic_info.partitions),
                    'replication_factor': len(list(topic_info.partitions.values())[0].replicas) if topic_info.partitions else 0
                }
            
            return metrics
            
        except Exception as e:
            print(f"❌ Error getting topic metrics: {e}")
            return {}
    
    def get_consumer_lag(self, group_id: str = "streaming-processor") -> Dict[str, int]:
        """⏱️ Get consumer lag metrics"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=f"{group_id}-monitor",
                enable_auto_commit=False
            )
            
            # Get topics
            topics = ['transactions-normal', 'transactions-suspicious', 'transactions-fraud-alerts']
            
            lag_metrics = {}
            for topic in topics:
                try:
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        
                        # Get high water marks
                        high_water_marks = consumer.end_offsets(topic_partitions)
                        
                        # Get committed offsets
                        committed_offsets = consumer.committed(*topic_partitions)
                        
                        total_lag = 0
                        for tp in topic_partitions:
                            high_water = high_water_marks.get(tp, 0)
                            committed = committed_offsets.get(tp, 0) or 0
                            lag = max(0, high_water - committed)
                            total_lag += lag
                        
                        lag_metrics[topic] = total_lag
                        
                except Exception as e:
                    lag_metrics[topic] = 0
            
            consumer.close()
            return lag_metrics
            
        except Exception as e:
            print(f"❌ Error getting consumer lag: {e}")
            return {}
    
    def print_metrics(self, topic_metrics: Dict, lag_metrics: Dict):
        """🖨️ Print formatted metrics"""
        now = datetime.now().strftime("%H:%M:%S")
        
        print(f"\n📊 Pipeline Status [{now}]")
        print("=" * 50)
        
        # Topic metrics
        print("📡 Topic Metrics:")
        for topic, metrics in topic_metrics.items():
            print(f"   {topic}: {metrics['partitions']} partitions")
        
        # Lag metrics
        print("\n⏱️ Consumer Lag:")
        total_lag = 0
        for topic, lag in lag_metrics.items():
            status = "🟢" if lag < 100 else "🟡" if lag < 1000 else "🔴"
            print(f"   {status} {topic}: {lag:,} messages")
            total_lag += lag
        
        print(f"\n📈 Total Lag: {total_lag:,} messages")
        
        # Health status
        if total_lag < 500:
            print("✅ Pipeline Status: HEALTHY")
        elif total_lag < 2000:
            print("⚠️ Pipeline Status: WARNING")
        else:
            print("🚨 Pipeline Status: CRITICAL")
    
    def monitor_loop(self, interval: int = 10):
        """🔄 Main monitoring loop"""
        print("🚀 Starting CryptoBridge Pipeline Monitor")
        print(f"🔄 Monitoring interval: {interval}s")
        print("Press Ctrl+C to stop")
        
        while self.running:
            try:
                # Get metrics
                topic_metrics = self.get_topic_metrics()
                lag_metrics = self.get_consumer_lag()
                
                # Print metrics
                self.print_metrics(topic_metrics, lag_metrics)
                
                # Wait for next iteration
                time.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Monitor error: {e}")
                time.sleep(5)

def main():
    """🎯 Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CryptoBridge Pipeline Monitor")
    parser.add_argument("--interval", type=int, default=10, help="Monitoring interval in seconds")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    
    args = parser.parse_args()
    
    monitor = PipelineMonitor(args.bootstrap_servers)
    monitor.monitor_loop(args.interval)

if __name__ == "__main__":
    main()