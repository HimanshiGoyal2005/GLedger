import datetime
from datetime import timedelta

import pathway as pw

HOURLY_EMISSION_LIMIT_KG = 500.0  # Max kg CO2 per hour
DAILY_EMISSION_LIMIT_KG = 10000.0  # Max kg CO2 per day
EFFICIENCY_MIN = 10.0  # Min kg CO2 per production unit
EFFICIENCY_MAX = 20.0  # Max kg CO2 per production unit (warning level)

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

factory_stream = FactoryStream(
    pw.io.stdin.read(
        schema=InputSchema,
        format="json",
        autocommit_duration_ms=1000,
    )
)

factory_stream = factory_stream.select(
    plant_id=pw.this.plant_id,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%f"),
    energy_kwh=pw.this.energy_kwh,
    fuel_liters=pw.this.fuel_liters,
    production_units=pw.this.production_units,
    temperature=pw.this.temperature,
    carbon_kg=(pw.this.energy_kwh * 0.82) + (pw.this.fuel_liters * 2.31),
)

class ComplianceRule:
    """Base class for compliance rules"""
    
    def __init__(self, name: str, description: str, severity: str):
        self.name = name
        self.description = description
        self.severity = severity

hourly_window = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.tumbling(timedelta(hours=1)),
).groupby(pw.this.plant_id).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    plant_id=pw.this._pw_grouping_key,
    hourly_carbon=pw.reducers.sum(pw.this.carbon_kg),
    reading_count=pw.reducers.count(),
)

hourly_violations = hourly_window.filter(
    pw.this.hourly_carbon > HOURLY_EMISSION_LIMIT_KG
)

hourly_violations = hourly_violations.select(
    pw.this.plant_id,
    window_start=pw.this.window_start,
    window_end=pw.this.window_end,
    hourly_carbon=pw.this.hourly_carbon,
    limit=HOURLY_EMISSION_LIMIT_KG,
    rule_name=pw.literal("HOURLY_EMISSION_LIMIT"),
    description=pw.literal("Hourly emissions exceed permitted limit"),
    severity=pw.literal("CRITICAL"),
    compliance_status=pw.literal("NON_COMPLIANT"),
)

daily_window = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.tumbling(timedelta(days=1)),
).groupby(pw.this.plant_id).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    plant_id=pw.this._pw_grouping_key,
    daily_carbon=pw.reducers.sum(pw.this.carbon_kg),
    total_production=pw.reducers.sum(pw.this.production_units),
    reading_count=pw.reducers.count(),
)

daily_violations = daily_window.filter(
    pw.this.daily_carbon > DAILY_EMISSION_LIMIT_KG
)

daily_violations = daily_violations.select(
    pw.this.plant_id,
    date=pw.this.window_start.date(),
    daily_carbon=pw.this.daily_carbon,
    limit=DAILY_EMISSION_LIMIT_KG,
    rule_name=pw.literal("DAILY_EMISSION_LIMIT"),
    description=pw.literal("Daily emissions exceed permitted limit"),
    severity=pw.literal("CRITICAL"),
    compliance_status=pw.literal("NON_COMPLIANT"),
)

factory_efficiency = factory_stream.select(
    plant_id=pw.this.plant_id,
    timestamp=pw.this.timestamp,
    carbon_kg=pw.this.carbon_kg,
    production_units=pw.this.production_units,
    efficiency=(
        pw.this.carbon_kg / pw.this.production_units
        if pw.this.production_units > 0
        else 0
    ),
)

inefficiency_violations = factory_efficiency.filter(
    pw.this.efficiency > EFFICIENCY_MAX
)

inefficiency_violations = inefficiency_violations.select(
    pw.this.plant_id,
    pw.this.timestamp,
    carbon_kg=pw.this.carbon_kg,
    production_units=pw.this.production_units,
    efficiency=pw.this.efficiency,
    min_efficiency=EFFICIENCY_MIN,
    max_efficiency=EFFICIENCY_MAX,
    rule_name=pw.literal("EFFICIENCY_MINIMUM"),
    description=pw.literal("Production efficiency below minimum threshold"),
    severity=pw.literal("WARNING"),
    compliance_status=pw.literal("NEEDS_IMPROVEMENT"),
)

compliance_window = factory_stream.windowby(
    factory_stream.timestamp,
    window=pw.temporal.sliding(
        hop=timedelta(minutes=5),
        duration=timedelta(hours=1),
    ),
    behavior=pw.temporal.common_behavior(
        cutoff=timedelta(seconds=30),
        keep_results=True,
    ),
).groupby(pw.this.plant_id).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    plant_id=pw.this._pw_grouping_key,
    
    total_carbon=pw.reducers.sum(pw.this.carbon_kg),
    total_production=pw.reducers.sum(pw.this.production_units),
    reading_count=pw.reducers.count(),

compliance_window = compliance_window.select(
    pw.this.window_start,
    pw.this.window_end,
    pw.this.plant_id,
    pw.this.total_carbon,
    pw.this.total_production,
    pw.this.reading_count,
    efficiency=(
        pw.this.total_carbon / pw.this.total_production
        if pw.this.total_production > 0
        else 0
    ),
)

compliance_window = compliance_window.select(
    pw.this.window_start,
    pw.this.window_end,
    pw.this.plant_id,
    pw.this.total_carbon,
    pw.this.total_production,
    pw.this.efficiency,
 
    compliance_score=(
        100 - (pw.this.efficiency - EFFICIENCY_MIN) * 10
        if pw.this.efficiency > EFFICIENCY_MIN
        else 50
    ),
)


latest_reading = factory_stream.groupby(pw.this.plant_id).reduce(
    plant_id=pw.this._pw_grouping_key,
    latest_timestamp=pw.this.timestamp.dt.max,
    latest_carbon=pw.reducers.sum(pw.this.carbon_kg),
    total_production=pw.reducers.sum(pw.this.production_units),
)

latest_reading = latest_reading.select(
    pw.this.plant_id,
    pw.this.latest_timestamp,
    pw.this.latest_carbon,
    pw.this.total_production,
   
    is_compliant=(
        pw.this.latest_carbon <= HOURLY_EMISSION_LIMIT_KG
    ),
    efficiency=(
        pw.this.latest_carbon / pw.this.total_production
        if pw.this.total_production > 0
        else 0
    ),
)

pw.io.stdout.write(
    hourly_violations,
    format="json",
)

pw.io.stdout.write(
    daily_violations,
    format="json",
)

pw.io.stdout.write(
    inefficiency_violations,
    format="json",
)

pw.io.stdout.write(
    compliance_window,
    format="json",
)

pw.io.stdout.write(
    latest_reading,
    format="json",
)


def on_hourly_violation(key, row, time, is_addition):
    """Handle hourly violation events"""
    if is_addition:
        print(f"🔴 CRITICAL: {row['plant_id']} - Hourly limit exceeded! "
              f"{row['hourly_carbon']:.1f}kg / {row['limit']}kg limit")


def on_daily_violation(key, row, time, is_addition):
    """Handle daily violation events"""
    if is_addition:
        print(f"🔴 CRITICAL: {row['plant_id']} - Daily limit exceeded! "
              f"{row['daily_carbon']:.1f}kg / {row['limit']}kg limit")


def on_inefficiency(key, row, time, is_addition):
    """Handle inefficiency events"""
    if is_addition:
        print(f"⚠️  WARNING: {row['plant_id']} - Low efficiency! "
              f"{row['efficiency']:.1f}kg CO2/unit")


pw.io.subscribe(hourly_violations, on_hourly_violation)
pw.io.subscribe(daily_violations, on_daily_violation)
pw.io.subscribe(inefficiency_violations, on_inefficiency)


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="GreenLedger Compliance Engine")
    parser.add_argument("--license-key", type=str, default=None,
                        help="Pathway license key")
    args = parser.parse_args()
    
    if args.license_key:
        pw.set_license_key(args.license_key)
    else:
        pw.set_license_key("demo-license-key-with-telemetry")
    
    print("Starting GreenLedger Compliance Engine...", file=sys.stderr)
    print(f"Hourly limit: {HOURLY_EMISSION_LIMIT_KG}kg", file=sys.stderr)
    print(f"Daily limit: {DAILY_EMISSION_LIMIT_KG}kg", file=sys.stderr)
    print(f"Efficiency range: {EFFICIENCY_MIN}-{EFFICIENCY_MAX}kg CO2/unit", file=sys.stderr)
    
    pw.run()
