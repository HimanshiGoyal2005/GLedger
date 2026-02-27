# README
🌱 GreenLedger – Real‑Time Carbon Accountability Engine
GLedger is a real‑time, AI‑powered carbon emissions monitoring and compliance platform designed for industrial sustainability. It transforms carbon tracking from delayed, batch‑based ESG reporting into a live, explainable, and actionable system.

The project continuously monitors emissions across multiple industrial plants using streaming data and provides real‑time insights, efficiency metrics, and compliance alerts.

# 🚀Features
Real-time carbon emissions monitoring for 4 industrial plants
Carbon emissions chart over time
Production efficiency metrics and charts
Plant comparison table
Efficiency leaderboard with compliance ratings
Violation alerts display
Demo Mode (enabled by default) that generates sample data

# 🛠Technology Stack
Language: Python
Streaming Engine: Pathway
Data Processing: Pandas
Environment: Linux (Ubuntu / WSL)
Data Source: Simulated live telemetry


# How to Run
1️⃣ Setup Environment

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2️⃣ Run Demo Pipeline

python -m green_ledger.demo_pipeline --rows 50 --rate 10

3️⃣ Static Mode (Optional)

python -m green_ledger.demo_pipeline --static


# 🌍Why GreenLedger?

Most sustainability tools focus on reporting after the damage is done.
GreenLedger enables:

Proactive carbon governance

Real‑time accountability

Explainable compliance decisions

Scalable, production‑ready design


It treats carbon as a live operational signal, not just a reporting metric.

# 🔮Future Enhancements

Real IoT sensor integration

Carbon credit simulation

Advanced dashboards

Multi‑region compliance rules


# 👥Team

Himanshi(Team Leader)
Srishti


