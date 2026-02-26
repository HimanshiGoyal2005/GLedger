#!/usr/bin/env python3
"""
GreenLedger Anomaly Detector
Real-time emission spike detection and threshold violation alerts
"""

import datetime
from datetime import timedelta
from typing import Optional

import pathway as pw


# =============================================================================
# Configuration
# =============================================================================

# Threshold: emission > 500kg/hour = violation
EMISSION_THRESHOLD_KG = 500.0

# Rolling statistics window
STATS_WINDOW_DURATION = timedelta(minutes=10)
STATS_WINDOW_HOP = timedelta(seconds=30)


# =============================================================================
# Input Schema
# =============================================================================

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


@pw.table(with_versions=True)
class FactoryStream:
    plant_id: str
    timestamp: datetime.datetime
    energy_kwh: float
    fuel_liters: float
    production_units: int
    temperature: float


# Read from stdin
factory_stream = FactoryStream(
    pw.io.stdin.read(
        schema=InputSchema,
        format="json",
        autocommit_duration_ms=1000,
    )
)

# Parse timestamp and calculate carbon
factory_stream = factory_stream.select(
    plant_id=pw.this.plant_id,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%f"),
    energy_kwh=pw.this.energy_kwh,
    fuel_liters=pw.this.fuel_liters,
    production_units=pw.this.production_units,
    temperature=pw.this.temperature,
    carbon_kg=(pw.this.energy_kwh * 0.82) + (pw.this.fuel_liters * 2.31),
)


# =============================================================================
# Anomaly Detection: Threshold Violations
# =============================================================================

# Filter for readings that exceed the threshold
violations = factory_stream.filter(
    factory_stream.carbon_kg > EMISSION_THRESHOLD_KG
)

violations = violations.select(
    pw.this.plant_id,
    pw.this.timestamp,
    pw.this.carbon_kg,
    pw.this.energy_kwh,
    pw.this.fuel_liters,
    violation_type=pw.literal("THRESHOLD_EXCEEDED"),
    severity=pw.literal("HIGH"),
    message=f"Emission {pw.this.carbon_kg:.1f}kg exceeds threshold {EMISSION_THRESHOLD_KG}kg",
)


# =============================================================================
# Rolling Statistics for Spike Detection
# =============================================================================

# Calculate rolling statistics per plant
rolling_stats = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.sliding(
        hop=STATS_WINDOW_HOP,
        duration=STATS_WINDOW_DURATION,
    ),
    behavior=pw.temporal.common_behavior(
        cutoff=timedelta(seconds=30),
        keep_results=True,
    ),
).groupby(pw.this.plant_id).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    plant_id=pw.this._pw_grouping_key,
    
    # Rolling statistics
    mean_carbon=pw.reducers.mean(pw.this.carbon_kg),
    std_carbon=pw.reducers.std(pw.this.carbon_kg),
    max_carbon=pw.reducers.max(pw.this.carbon_kg),
    min_carbon=pw.reducers.min(pw.this.carbon_kg),
    reading_count=pw.reducers.count(),
    
    # Current values
    current_carbon=pw.reducers.last(pw.this.carbon_kg),
    current_energy=pw.reducers.last(pw.this.energy_kwh),
    current_fuel=pw.reducers.last(pw.this.fuel_liters),
)


# Calculate z-score for spike detection (anomalies > 2 std deviations)
rolling_stats = rolling_stats.select(
    pw.this.window_start,
    pw.this.window_end,
    pw.this.plant_id,
    pw.this.mean_carbon,
    pw.this.std_carbon,
    pw.this.max_carbon,
    pw.this.min_carbon,
    pw.this.reading_count,
    pw.this.current_carbon,
    pw.this.current_energy,
    pw.this.current_fuel,
    
    # Z-score: how many standard deviations from mean
    z_score=(
        (pw.this.current_carbon - pw.this.mean_carbon) / pw.this.std_carbon
        if pw.this.std_carbon > 0
        else 0
    ),
    
    # Flag as spike if z-score > 2
    is_spike=(
        (pw.this.current_carbon - pw.this.mean_carbon) / pw.this.std_carbon > 2.0
        if pw.this.std_carbon > 0
        else False
    ),
)


# Filter for spikes
spikes = rolling_stats.filter(pw.this.is_spike)

spikes = spikes.select(
    pw.this.plant_id,
    window_end=pw.this.window_end,
    current_carbon=pw.this.current_carbon,
    mean_carbon=pw.this.mean_carbon,
    std_carbon=pw.this.std_carbon,
    z_score=pw.this.z_score,
    violation_type=pw.literal("SPIKE_DETECTED"),
    severity=pw.literal("MEDIUM"),
    message=f"Spike detected: {pw.this.z_score:.1f}σ above mean",
)


# =============================================================================
# High Temperature Alerts
# =============================================================================

# Alert if temperature exceeds threshold
temp_alerts = factory_stream.filter(
    factory_stream.temperature > 35  # Celsius
)

temp_alerts = temp_alerts.select(
    pw.this.plant_id,
    pw.this.timestamp,
    pw.this.temperature,
    violation_type=pw.literal("HIGH_TEMPERATURE"),
    severity=pw.literal("LOW"),
    message=f"Temperature {pw.this.temperature:.1f}°C exceeds normal range",
)


# =============================================================================
# Low Production Efficiency Alerts
# =============================================================================

# Calculate efficiency (carbon per unit produced)
factory_with_efficiency = factory_stream.select(
    pw.this.plant_id,
    pw.this.timestamp,
    pw.this.carbon_kg,
    pw.this.production_units,
    efficiency=(
        pw.this.carbon_kg / pw.this.production_units
        if pw.this.production_units > 0
        else 0
    ),
)

# Alert if efficiency drops below threshold (high carbon per unit)
efficiency_threshold = 15.0  # kg CO2 per unit
low_efficiency = factory_with_efficiency.filter(
    pw.this.efficiency > efficiency_threshold
)

low_efficiency = low_efficiency.select(
    pw.this.plant_id,
    pw.this.timestamp,
    carbon_kg=pw.this.carbon_kg,
    production_units=pw.this.production_units,
    efficiency=pw.this.efficiency,
    violation_type=pw.literal("LOW_EFFICIENCY"),
    severity=pw.literal("MEDIUM"),
    message=f"Low efficiency: {pw.this.efficiency:.1f}kg CO2/unit (threshold: {efficiency_threshold})",
)


# =============================================================================
# Output Streams
# =============================================================================

# Write violations
pw.io.stdout.write(
    violations,
    format="json",
)

# Write spikes
pw.io.stdout.write(
    spikes,
    format="json",
)

# Write temperature alerts
pw.io.stdout.write(
    temp_alerts,
    format="json",
)

# Write efficiency alerts
pw.io.stdout.write(
    low_efficiency,
    format="json",
)


# =============================================================================
# Subscription-based real-time alerts
# =============================================================================

def on_violation(key, row, time, is_addition):
    """Handle violation events"""
    if is_addition:
        print(f"🚨 ALERT: {row['plant_id']} at {row['timestamp']}: {row['message']}")


def on_spike(key, row, time, is_addition):
    """Handle spike events"""
    if is_addition:
        print(f"⚠️  SPIKE: {row['plant_id']} - z-score: {row['z_score']:.2f}")


# Subscribe to violations and spikes for real-time alerts
pw.io.subscribe(violations, on_violation)
pw.io.subscribe(spikes, on_spike)


# =============================================================================
# Run the pipeline
# =============================================================================

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="GreenLedger Anomaly Detector")
    parser.add_argument("--license-key", type=str, default=None,
                        help="Pathway license key")
    parser.add_argument("--threshold", type=float, default=EMISSION_THRESHOLD_KG,
                        help="Emission threshold in kg")
    args = parser.parse_args()
    
    if args.license_key:
        pw.set_license_key(args.license_key)
    else:
        pw.set_license_key("demo-license-key-with-telemetry")
    
    print("Starting GreenLedger Anomaly Detector...", file=sys.stderr)
    print(f"Threshold: {args.threshold}kg CO2", file=sys.stderr)
    print(f"Z-score threshold: 2.0 standard deviations", file=sys.stderr)
    
    pw.run()
