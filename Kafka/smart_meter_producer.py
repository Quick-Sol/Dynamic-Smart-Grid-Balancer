# smart_meter_producer.py
# Simulates millions of homes sending power readings every 10 seconds

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'  # Update with your Kafka cluster
TOPIC = 'smart-meter-readings'
ZIP_CODES = [
    '90210', '10001', '60601', '77001', '30309',  # Major metros
    '85001', '33101', '98101', '02101', '19103',
    '75201', '80202', '94102', '20001', '85004',
    # ... add more zip codes to simulate millions
]

class SmartMeterSimulator:
    def __init__(self, num_homes=1_000_000):
        self.num_homes = num_homes
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            batch_size=65536,           # 64KB batches for throughput
            linger_ms=100,              # Batch for 100ms
            compression_type='lz4',     # Fast compression
            acks=1,                     # Leader acknowledgment only
            retries=3,
            max_in_flight_requests=5
        )
        
        # Base load patterns by time of day (kW)
        self.base_patterns = {
            'morning_peak': (2.5, 4.0),    # 6-9 AM
            'midday': (1.5, 2.5),          # 9 AM-5 PM
            'evening_peak': (4.0, 7.0),    # 5-9 PM
            'night': (0.8, 1.5),           # 9 PM-6 AM
        }
        
        # Weather simulation for heatwave scenarios
        self.heatwave_active = False
        self.heatwave_start = None
        
    def get_time_period(self, hour):
        if 6 <= hour < 9:
            return 'morning_peak'
        elif 9 <= hour < 17:
            return 'midday'
        elif 17 <= hour < 21:
            return 'evening_peak'
        else:
            return 'night'
    
    def generate_reading(self, home_id, zip_code):
        """Generate realistic power consumption reading"""
        now = datetime.utcnow()
        hour = now.hour
        
        # Base load by time of day
        period = self.get_time_period(hour)
        base_min, base_max = self.base_patterns[period]
        
        # Add randomness and home-specific characteristics
        home_seed = hash(home_id) % 1000
        base_load = base_min + (base_max - base_min) * (home_seed / 1000)
        
        # Weather impact (AC usage during heatwave)
        weather_multiplier = 1.0
        if self.heatwave_active:
            # AC spikes: 2-4x normal load during heatwave
            if period in ['midday', 'evening_peak']:
                weather_multiplier = random.uniform(2.5, 4.5)
            else:
                weather_multiplier = random.uniform(1.5, 2.5)
        
        # Add noise and occasional spikes (faulty appliances, etc.)
        noise = random.gauss(0, 0.1)
        spike = random.random() < 0.001  # 0.1% chance of spike
        spike_factor = random.uniform(3.0, 8.0) if spike else 1.0
        
        power_kw = max(0.1, base_load * weather_multiplier * spike_factor + noise)
        
        # Simulate grid stress events
        grid_stress = 0
        if self.heatwave_active and period == 'evening_peak':
            # Correlated surge during heatwave evening
            grid_stress = random.uniform(0.7, 1.0)
        
        reading = {
            'meter_id': f'meter_{home_id:010d}',
            'zip_code': zip_code,
            'timestamp': now.isoformat(),
            'power_kw': round(power_kw, 3),
            'voltage': round(random.uniform(228, 252), 1),  # 240V ±5%
            'frequency': round(random.uniform(59.95, 60.05), 2),
            'temperature_f': round(random.uniform(75, 115) if self.heatwave_active else random.uniform(65, 85), 1),
            'grid_stress_indicator': round(grid_stress, 2),
            'reading_interval_sec': 10
        }
        
        return reading
    
    def trigger_heatwave(self, duration_hours=6):
        """Simulate a heatwave event causing massive AC usage"""
        self.heatwave_active = True
        self.heatwave_start = datetime.utcnow()
        print(f"🔥 HEATWAVE TRIGGERED at {self.heatwave_start}")
        
        # Auto-end heatwave after duration
        def end_heatwave():
            time.sleep(duration_hours * 3600)
            self.heatwave_active = False
            print(f"🌡️ Heatwave ended")
        
        import threading
        threading.Thread(target=end_heatwave, daemon=True).start()
    
    def send_batch(self, batch_size=1000):
        """Send a batch of readings to Kafka"""
        futures = []
        for _ in range(batch_size):
            home_id = random.randint(1, self.num_homes)
            zip_code = random.choice(ZIP_CODES)
            reading = self.generate_reading(home_id, zip_code)
            
            # Partition by zip_code for locality
            key = reading['zip_code']
            future = self.producer.send(TOPIC, key=key, value=reading)
            futures.append(future)
        
        # Wait for batch to complete
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                print(f"Failed to send: {e}")
    
    def run(self, target_tps=100_000):
        """Run continuous simulation at target transactions per second"""
        print(f"🚀 Starting simulation: {self.num_homes:,} homes @ {target_tps:,} TPS")
        
        batch_size = 1000
        interval = batch_size / target_tps
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            while True:
                executor.submit(self.send_batch, batch_size)
                time.sleep(interval)
    
    def close(self):
        self.producer.flush()
        self.producer.close()


# Run simulation
if __name__ == '__main__':
    simulator = SmartMeterSimulator(num_homes=5_000_000)  # 5M homes
    
    # Optional: Trigger heatwave after 2 minutes for testing
    import threading
    threading.Timer(120, lambda: simulator.trigger_heatwave(duration_hours=4)).start()
    
    try:
        simulator.run(target_tps=500_000)  # 500K readings/sec
    except KeyboardInterrupt:
        simulator.close()
        print("Simulation stopped")
 
