# GreenLedger - Real-Time Carbon Accountability Engine

## Project Plan

### 1. Information Gathered

**From test_pathway.py:**
- Basic Pathway table definition and carbon calculation works
- Formula: carbon = energy_kwh * 0.82 + fuel_liters * 2.31

**From pathway examples (alerts.py):**
- Streaming with Kafka connector
- Sliding window pattern: `pw.temporal.sliding()`
- Alert threshold detection
- Real-time subscription for notifications

**Architecture Requirements:**
- Simulated live data stream (multiple factories)
- Real-time carbon calculation with rolling windows
- Anomaly detection (threshold violations)
- Compliance document store
- LLM integration for explanations
- Dashboard

### 2. Detailed Plan

#### Phase 1: Real-Time Pipeline (DAY 1)
- [ ] `data_simulator.py` - Simulate live factory data stream
  - Schema: plant_id, timestamp, energy_kwh, fuel_liters, production_units, temperature
  - Multiple factories (Plant A, B, C, D)
  - Streaming output to stdout/Kafka
  
- [ ] `carbon_pipeline.py` - Core streaming carbon calculation
  - Pathway streaming input
  - Carbon formula: `(energy_kwh × 0.82) + (fuel_liters × 2.31)`
  - Rolling 10-minute window
  - Emission per production unit
  - Daily total aggregation

#### Phase 2: Anomaly + Compliance Engine (DAY 2)
- [ ] `anomaly_detector.py` - Emission spike detection
  - Threshold: > 500kg/hour = violation
  - Rolling statistics (mean, std)
  - Alert generation
  
- [ ] `compliance_engine.py` - Compliance rule engine
  - Check against thresholds
  - Generate violation records
  
- [ ] `documents/` - Compliance document store
  - carbon_policy.txt
  - emission_standards.md
  - esg_compliance.md

#### Phase 3: LLM Integration (DAY 3)
- [ ] `rag_engine.py` - Retrieval augmented generation
  - Document indexing
  - Query engine
  - Context retrieval
  
- [ ] `explanation_service.py` - LLM explanations
  - "Why did Plant A violate?"
  - "Summarize today's carbon activity"
  - "Which plant is least efficient?"

#### Phase 4: Dashboard (DAY 4)
- [ ] `dashboard.py` - Simple Streamlit dashboard
  - Carbon emissions graph per plant
  - Alert list
  - Emission leaderboard

### 3. File Structure

```
GLedger/
├── PLAN.md (this file)
├── test_pathway.py (existing)
├── data_simulator.py       # Phase 1
├── carbon_pipeline.py      # Phase 1
├── anomaly_detector.py     # Phase 2
├── compliance_engine.py    # Phase 2
├── rag_engine.py           # Phase 3
├── explanation_service.py  # Phase 3
├── dashboard.py            # Phase 4
├── documents/
│   ├── carbon_policy.txt
│   ├── emission_standards.md
│   └── esg_compliance.md
└── requirements.txt
```

### 4. Dependencies

- pathway
- streamlit (dashboard)
- python-dotenv
- langchain (LLM)
- chromadb (vector store for RAG)

### 5. Follow-up Steps

1. Create requirements.txt
2. Create data_simulator.py for Phase 1
3. Test streaming carbon calculation
4. Add anomaly detection
5. Build compliance engine
6. Integrate LLM
7. Build dashboard
