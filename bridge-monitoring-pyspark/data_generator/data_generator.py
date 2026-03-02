"""
Bridge Sensor Data Generator

This module simulates IoT sensor data for bridge monitoring systems.
Generates temperature, vibration, and tilt sensor readings for multiple bridges
with configurable rate and duration.

Usage:
    python data_generator.py --duration 600 --rate 1
"""

import json
import time
import random
from datetime import datetime, timedelta
import os
import argparse

class BridgeSensorGenerator:
    """
    Simulates IoT sensors for bridge monitoring.
    
    Attributes:
        output_dir (str): Directory where sensor data will be written
        rate (float): Number of events per second per sensor type
        bridge_ids (list): List of bridge identifiers
        event_counter (int): Counter for total events generated
    """
    
    def __init__(self, output_dir="streams", rate=1):
        """
        Initialize the sensor data generator.
        
        Args:
            output_dir (str): Output directory for sensor data
            rate (float): Events per second per sensor type
        """
        self.output_dir = output_dir
        self.rate = rate
        self.bridge_ids = ["B001", "B002", "B003", "B004", "B005"]
        self.event_counter = 0
        
    def generate_event(self, sensor_type):
        """
        Generate a single sensor event with simulated late arrival.
        
        Args:
            sensor_type (str): Type of sensor (temperature, vibration, or tilt)
            
        Returns:
            dict: Sensor event with timestamp, bridge_id, sensor_type, and value
        """
        # Add random lag (0-60 seconds) for late arrivals
        lag = random.randint(0, 60)
        event_time = datetime.now() - timedelta(seconds=lag)
        
        # Generate sensor-specific values
        if sensor_type == "temperature":
            value = random.uniform(-10, 50)
        elif sensor_type == "vibration":
            value = random.uniform(0, 100)
        else:  # tilt
            value = random.uniform(0, 5)
        
        return {
            "event_time": event_time.isoformat(),
            "bridge_id": random.choice(self.bridge_ids),
            "sensor_type": sensor_type,
            "value": round(value, 2),
            "ingest_time": datetime.now().isoformat()
        }
    
    def write_events(self, duration=300):
        """
        Continuously write sensor events for specified duration.
        
        Args:
            duration (int): Duration in seconds to generate events
        """
        sensor_types = ["temperature", "vibration", "tilt"]
        start_time = time.time()
        
        print(f"Starting data generator for {duration} seconds...")
        print(f"Output directory: {self.output_dir}")
        print(f"Rate: {self.rate} events per second per sensor")
        print("=" * 60)
        
        try:
            while time.time() - start_time < duration:
                batch_start = time.time()
                
                for sensor_type in sensor_types:
                    event = self.generate_event(sensor_type)
                    timestamp = int(time.time() * 1000)
                    output_path = os.path.join(
                        self.output_dir, 
                        f"bridge_{sensor_type}",
                        f"event_{timestamp}_{random.randint(1000, 9999)}.json"
                    )
                    
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    with open(output_path, 'w') as f:
                        json.dump(event, f, indent=2)
                    
                    self.event_counter += 1
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Event #{self.event_counter:04d} | "
                          f"{sensor_type:12s} | "
                          f"Bridge: {event['bridge_id']} | "
                          f"Value: {event['value']:6.2f}")
                
                elapsed = time.time() - start_time
                remaining = duration - elapsed
                
                if self.event_counter % 15 == 0:
                    print(f"\n--- Progress: {elapsed:.0f}s elapsed, "
                          f"{remaining:.0f}s remaining, "
                          f"{self.event_counter} total events ---\n")
                
                sleep_time = (1.0 / self.rate) - (time.time() - batch_start)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            print("\n\nGenerator stopped by user.")
        finally:
            elapsed_total = time.time() - start_time
            print("\n" + "=" * 60)
            print(f"Generator finished!")
            print(f"Total events generated: {self.event_counter}")
            print(f"Total duration: {elapsed_total:.2f} seconds")
            print(f"Average rate: {self.event_counter / elapsed_total:.2f} events/sec")
            print("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate bridge sensor data')
    parser.add_argument("--duration", type=int, default=300, 
                        help="Duration in seconds (default: 300)")
    parser.add_argument("--rate", type=float, default=1.0, 
                        help="Events per second per sensor (default: 1.0)")
    parser.add_argument("--output", type=str, default="streams",
                        help="Output directory (default: streams)")
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print("BRIDGE SENSOR DATA GENERATOR")
    print("=" * 60)
    
    generator = BridgeSensorGenerator(output_dir=args.output, rate=args.rate)
    generator.write_events(duration=args.duration)
