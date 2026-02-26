# Emission Standards - Technical Specification

## Carbon Emission Calculation Formula

The standard formula for calculating carbon emissions is:

```
carbon_emission (kg CO2) = (energy_kwh × 0.82) + (fuel_liters × 2.31)
```

### Emission Factors
- **Grid Electricity**: 0.82 kg CO2 per kWh
- **Diesel Fuel**: 2.31 kg CO2 per liter
- **Natural Gas**: 2.0 kg CO2 per cubic meter (future expansion)

## Measurement Standards

### Data Collection
- **Frequency**: Minimum 1 reading per minute
- **Required Fields**:
  - plant_id: Unique facility identifier
  - timestamp: ISO 8601 format
  - energy_kwh: Kilowatt-hours consumed
  - fuel_liters: Liters of fuel consumed
  - production_units: Units produced
  - temperature: Ambient temperature in Celsius

### Accuracy Requirements
- Energy measurement: ±2% accuracy
- Fuel measurement: ±5% accuracy
- Timestamp: ±1 second synchronization

## Aggregation Windows

### Real-Time Windows
- **1-minute window**: Immediate alerts
- **10-minute window**: Rolling average for trend analysis
- **1-hour window**: Compliance checking
- **24-hour window**: Daily totals and reporting

### Window Behavior
- Sliding windows with 1-minute hop
- Cutoff: 30 seconds for late data
- Keep results: true for comparison

## Alert Thresholds

| Alert Level | Threshold | Action |
|-------------|-----------|--------|
| INFO | > 300 kg/hr | Log only |
| WARNING | > 400 kg/hr | Notify plant manager |
| CRITICAL | > 500 kg/hr | Immediate alert |
| EMERGENCY | > 1000 kg/hr | Auto-shutdown trigger |

## Efficiency Metrics

### Carbon Intensity
- **Formula**: carbon_kg / production_units
- **Unit**: kg CO2 per production unit
- **Target**: < 15 kg CO2/unit
- **Excellent**: < 10 kg CO2/unit

### Benchmarking
- Compare plants of similar size and type
- Industry average: 18-25 kg CO2/unit
- Best practice: < 10 kg CO2/unit

## Data Retention

- **Raw data**: 90 days
- **Hourly aggregates**: 2 years
- **Daily aggregates**: 7 years
- **Compliance reports**: Permanent
