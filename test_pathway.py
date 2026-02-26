import pathway as pw

@pw.table
class Input:
    plant_id: str
    energy_kwh: float
    fuel_liters: float

data = pw.debug.table_from_markdown("""
plant_id | energy_kwh | fuel_liters
A        | 100        | 20
B        | 150        | 30
""")

def carbon(e, f):
    return e * 0.82 + f * 2.31

result = data.select(
    plant_id=pw.this.plant_id,
    carbon_emission=carbon(pw.this.energy_kwh, pw.this.fuel_liters)
)

pw.debug.compute_and_print(result)
