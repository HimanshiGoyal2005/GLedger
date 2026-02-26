#!/usr/bin/env python3
"""
GreenLedger Carbon Pipeline
Real-time carbon emission calculation with streaming and windowing
"""

import datetime
from datetime import timedelta

import pathway as pw


# Carbon calculation formula
# carbon_emission = (energy_kwh × 0.82) + (fuel_liters × 2.31)
def calculate_carbon(energy_kwh: float, fuel_liters: float) -> float:
    """Calculate carbon emissions in kg"""
    return (energy_kwh * 0.82) + (fuel_liters * 2.31)


# Define input schema matching the data simulator output
InputSchema = pw.schema_builder(
    columns={
        "plant_id": pw.column_definition(dtype=str),
        "timestamp": pw.column_definition(dtype=str),
        "energy_kwh": pw.column_definition(dtype=float),
        "fuel_liters": pw.column_definition(dtype=float),
        "production_units": pw.column_definition(dtype=int),
        "temperature": pw.column_definition(dtype=float),
    }
)


# Read from stdin (simulating stream input)
# In production, this would be Kafka or another stream source
@pw.table(with_versions=True)
class FactoryStream:
    plant_id: str
    timestamp: datetime.datetime
    energy_kwh: float
    fuel_liters: float
    production_units: int
    temperature: float


# Parse the input data
factory_stream = FactoryStream(
    pw.io.stdin.read(
        schema=InputSchema,
        format="json",
        autocommit_duration_ms=1000,
    )
)

# Parse timestamp
factory_stream = factory_stream.select(
    plant_id=pw.this.plant_id,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%f"),
    energy_kwh=pw.this.energy_kwh,
    fuel_liters=pw.this.fuel_liters,
    production_units=pw.this.production_units,
    temperature=pw.this.temperature,
)

# Calculate instantaneous carbon emission
factory_stream = factory_stream.select(
    pw.this.plant_id,
    pw.this.timestamp,
    pw.this.energy_kwh,
    pw.this.fuel_liters,
    pw.this.production_units,
    pw.this.temperature,
    carbon_kg=calculate_carbon(pw.this.energy_kwh, pw.this.fuel_liters),
)


# =============================================================================
# Rolling 10-minute Window Aggregations
# =============================================================================

WINDOW_DURATION = timedelta(minutes=10)
WINDOW_HOP = timedelta(minutes=1)


# Windowed aggregations by plant
windowed_by_plant = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.sliding(
        hop=WINDOW_HOP,
        duration=WINDOW_DURATION,
    ),
    behavior=pw.temporal.common_behavior(
        cutoff=timedelta(seconds=30),
        keep_results=True,  # Keep for comparison
    ),
).groupby(
    pw.this.plant_id
).reduce(
    # Window metadata
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    plant_id=pw.this._pw_grouping_key,
    
    # Aggregated values
    total_energy_kwh=pw.reducers.sum(pw.this.energy_kwh),
    total_fuel_liters=pw.reducers.sum(pw.this.fuel_liters),
    total_production=pw.reducers.sum(pw.this.production_units),
    reading_count=pw.reducers.count(),
    
    # Carbon calculations
    total_carbon_kg=pw.reducers.sum(pw.this.carbon_kg),
    avg_carbon_per_reading=pw.reducers.mean(pw.this.carbon_kg),
    max_carbon_kg=pw.reducers.max(pw.this.carbon_kg),
    min_carbon_kg=pw.reducers.min(pw.this.carbon_kg),
)


# Calculate emission intensity (carbon per production unit)
windowed_by_plant = windowed_by_plant.select(
    pw.this.window_start,
    pw.this.window_end,
    pw.this.plant_id,
    pw.this.total_energy_kwh,
    pw.this.total_fuel_liters,
    pw.this.total_production,
    pw.this.reading_count,
    pw.this.total_carbon_kg,
    pw.this.avg_carbon_per_reading,
    pw.this.max_carbon_kg,
    pw.this.min_carbon_kg,
    
    # Carbon per production unit (efficiency metric)
    carbon_per_unit=(
        pw.this.total_carbon_kg / pw.this.total_production
        if pw.this.total_production > 0 
        else 0
    ),
)


# =============================================================================
# Global Window (All Plants Combined)
# =============================================================================

windowed_global = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.sliding(
        hop=WINDOW_HOP,
        duration=WINDOW_DURATION,
    ),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    
    total_energy_kwh=pw.reducers.sum(pw.this.energy_kwh),
    total_fuel_liters=pw.reducers.sum(pw.this.fuel_liters),
    total_production=pw.reducers.sum(pw.this.production_units),
    reading_count=pw.reducers.count(),
    total_carbon_kg=pw.reducers.sum(pw.this.carbon_kg),
    unique_plants=pw.reducers.count_unique(pw.this.plant_id),
)


# Calculate global efficiency
windowed_global = windowed_global.select(
    pw.this.window_start,
    pw.this.window_end,
    pw.this.total_energy_kwh,
    pw.this.total_fuel_liters,
    pw.this.total_production,
    pw.this.reading_count,
    pw.this.total_carbon_kg,
    pw.this.unique_plants,
    carbon_per_unit=(
        pw.this.total_carbon_kg / pw.this.total_production
        if pw.this.total_production > 0
        else 0
    ),
)


# =============================================================================
# Daily Totals (Tumbling Window for Daily Aggregation)
# =============================================================================

daily_by_plant = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.tumbling(datetime.timedelta(days=1)),
    behavior=pw.temporal.exactly_once_behavior(),
).groupby(pw.this.plant_id).reduce(
    date=pw.this._pw_window_start.date(),
    plant_id=pw.this._pw_grouping_key,
    daily_energy_kwh=pw.reducers.sum(pw.this.energy_kwh),
    daily_fuel_liters=pw.reducers.sum(pw.this.fuel_liters),
    daily_production=pw.reducers.sum(pw.this.production_units),
    daily_carbon_kg=pw.reducers.sum(pw.this.carbon_kg),
)


# =============================================================================
# Output Streams
# =============================================================================

# Write windowed results by plant to stdout
pw.io.stdout.write(
    windowed_by_plant,
    format="json",
)

# Write global windowed results
pw.io.stdout.write(
    windowed_global,
    format="json",
    output_path="global_window",
)

# Write daily results
pw.io.stdout.write(
    daily_by_plant,
    format="json",
    output_path="daily_summary",
)


# =============================================================================
# Run the pipeline
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="GreenLedger Carbon Pipeline")
    parser.add_argument("--license-key", type=str, default=None,
                        help="Pathway license key (optional)")
    args = parser.parse_args()
    
    if args.license_key:
        pw.set_license_key(args.license_key)
    else:
        # Use demo key for development
        pw.set_license_key("demo-license-key-with-telemetry")
    
    print("Starting GreenLedger Carbon Pipeline...", file=__import__('sys').stderr)
    print("Processing real-time carbon calculations with 10-minute rolling windows", file=__import__('sys').stderr)
    
    pw.run()
