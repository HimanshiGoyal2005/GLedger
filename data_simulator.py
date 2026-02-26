#!/usr/bin/env python3
"""
GreenLedger Data Simulator
Simulates real-time carbon emission data from multiple factories
"""

import json
import random
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Optional
import sys


@dataclass
class FactoryData:
    """Data schema for factory emissions"""
    plant_id: str
    timestamp: str
    energy_kwh: float
    fuel_liters: float
    production_units: int
    temperature: float


class CarbonDataSimulator:
    """Simulates real-time carbon emission data from multiple factories"""
    
    def __init__(self, plants: List[str] = None, base_interval: float = 1.0):
        self.plants = plants or ["Plant_A", "Plant_B", "Plant_C", "Plant_D"]
        self.base_interval = base_interval
        self.start_time = datetime.now()
        
        # Base values for each plant (to simulate different operational patterns)
        self.plant_profiles = {
            "Plant_A": {"energy_base": 100, "fuel_base": 20, "production_base": 50, "temp_base": 25},
            "Plant_B": {"energy_base": 150, "fuel_base": 35, "production_base": 75, "temp_base": 28},
            "Plant_C": {"energy_base": 80,  "fuel_base": 15, "production_base": 40, "temp_base": 22},
            "Plant_D": {"energy_base": 200, "fuel_base": 50, "production_base": 100, "temp_base": 30},
        }
        
    def generate_reading(self, plant_id: str, timestamp: datetime) -> FactoryData:
        """Generate a single reading for a plant with some random variation"""
        profile = self.plant_profiles.get(plant_id, self.plant_profiles["Plant_A"])
        
        # Add random variation (±20%)
        energy_kwh = profile["energy_base"] * random.uniform(0.8, 1.2)
        fuel_liters = profile["fuel_base"] * random.uniform(0.8, 1.2)
        production_units = int(profile["production_base"] * random.uniform(0.8, 1.2))
        temperature = profile["temp_base"] + random.uniform(-3, 3)
        
        # Occasionally generate spike (for anomaly detection testing)
        if random.random() < 0.05:  # 5% chance of spike
            energy_kwh *= 2.5
            fuel_liters *= 2.5
            
        return FactoryData(
            plant_id=plant_id,
            timestamp=timestamp.isoformat(),
            energy_kwh=round(energy_kwh, 2),
            fuel_liters=round(fuel_liters, 2),
            production_units=production_units,
            temperature=round(temperature, 1)
        )
    
    def calculate_carbon(self, energy_kwh: float, fuel_liters: float) -> float:
        """
        Carbon formula:
        carbon_emission = (energy_kwh × 0.82) + (fuel_liters × 2.31)
        """
        return (energy_kwh * 0.82) + (fuel_liters * 2.31)
    
    def stream_data(self, duration_seconds: Optional[int] = None, verbose: bool = True):
        """Stream data continuously"""
        start_time = time.time()
        iteration = 0
        
        print("Starting GreenLedger Data Simulator...", file=sys.stderr)
        print("Schema: plant_id, timestamp, energy_kwh, fuel_liters, production_units, temperature", file=sys.stderr)
        print("-" * 80, file=sys.stderr)
        
        try:
            while True:
                current_time = datetime.now()
                
                # Generate data for each plant
                for plant_id in self.plants:
                    data = self.generate_reading(plant_id, current_time)
                    carbon = self.calculate_carbon(data.energy_kwh, data.fuel_liters)
                    
                    # Output as JSON for easy parsing
                    output = {
                        "plant_id": data.plant_id,
                        "timestamp": data.timestamp,
                        "energy_kwh": data.energy_kwh,
                        "fuel_liters": data.fuel_liters,
                        "production_units": data.production_units,
                        "temperature": data.temperature,
                        "carbon_kg": round(carbon, 2)
                    }
                    
                    print(json.dumps(output), flush=True)
                    
                    if verbose and iteration % 10 == 0:
                        print(f"[{current_time.strftime('%H:%M:%S')}] {plant_id}: "
                              f"Energy={data.energy_kwh:.1f}kWh, "
                              f"Fuel={data.fuel_liters:.1f}L, "
                              f"Carbon={carbon:.1f}kg",
                              file=sys.stderr)
                
                iteration += 1
                time.sleep(self.base_interval)
                
                # Check duration limit
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                    
        except KeyboardInterrupt:
            print("\nStopping data simulator...", file=sys.stderr)
    
    def generate_batch(self, num_readings: int = 100) -> List[dict]:
        """Generate a batch of historical data"""
        results = []
        current_time = self.start_time
        
        for _ in range(num_readings):
            for plant_id in self.plants:
                data = self.generate_reading(plant_id, current_time)
                carbon = self.calculate_carbon(data.energy_kwh, data.fuel_liters)
                results.append({
                    "plant_id": data.plant_id,
                    "timestamp": data.timestamp,
                    "energy_kwh": data.energy_kwh,
                    "fuel_liters": data.fuel_liters,
                    "production_units": data.production_units,
                    "temperature": data.temperature,
                    "carbon_kg": round(carbon, 2)
                })
            current_time += timedelta(seconds=self.base_interval)
        
        return results


def main():
    """Main entry point for the data simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description="GreenLedger Data Simulator")
    parser.add_argument("--plants", nargs="+", default=["Plant_A", "Plant_B", "Plant_C", "Plant_D"],
                        help="List of plant IDs")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Interval between readings in seconds")
    parser.add_argument("--duration", type=int, default=None,
                        help="Duration to run in seconds (default: infinite)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress verbose output")
    parser.add_argument("--batch", type=int, default=0,
                        help="Generate batch mode (number of readings per plant)")
    
    args = parser.parse_args()
    
    simulator = CarbonDataSimulator(plants=args.plants, base_interval=args.interval)
    
    if args.batch > 0:
        # Batch mode - generate data and print
        data = simulator.generate_batch(args.batch)
        for row in data:
            print(json.dumps(row))
    else:
        # Streaming mode
        simulator.stream_data(duration_seconds=args.duration, verbose=not args.quiet)


if __name__ == "__main__":
    main()
