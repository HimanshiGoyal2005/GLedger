#!/usr/bin/env python3
"""
GreenLedger Explanation Service
LLM-powered explanations for compliance and carbon data
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

# Try to import LLM dependencies
try:
    from langchain_openai import ChatOpenAI
    from langchain.chains import LLMChain
    from langchain.prompts import PromptTemplate
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


# =============================================================================
# Configuration
# =============================================================================

# Default prompts
DEFAULT_CONTEXT_TEMPLATE = """You are GreenLedger, an AI assistant for carbon compliance monitoring.

You have access to:
1. Real-time emission data from multiple facilities
2. Compliance documents (carbon policy, emission standards, ESG guidelines)
3. Historical trends and anomalies

Based on the following context, answer the user's question.

Context:
{context}

Current Time: {current_time}

Question: {question}

Answer:"""

VIOLATION_EXPLANATION_TEMPLATE = """You are GreenLedger, a carbon compliance expert.

A facility has violated a compliance rule. Explain why this happened and what actions should be taken.

Violation Details:
- Plant: {plant_id}
- Violation Type: {violation_type}
- Value: {value}
- Threshold: {threshold}
- Time: {timestamp}

Relevant Policy:
{policy_context}

Provide a clear explanation of:
1. Why this violation occurred
2. Potential root causes
3. Recommended corrective actions
4. Expected timeline for resolution

Explanation:"""

EFFICIENCY_TEMPLATE = """You are GreenLedger, a sustainability analyst.

Analyze the efficiency of the following facilities:

{plant_data}

For each plant, identify:
1. Efficiency rating (kg CO2 per unit)
2. Comparison to benchmarks
3. Recommendations for improvement

Analysis:"""


class ExplanationService:
    """Service for generating LLM-powered explanations"""
    
    def __init__(self, model_name: str = "gpt-4", temperature: float = 0.7):
        self.model_name = model_name
        self.temperature = temperature
        self.llm = None
        self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize the LLM"""
        if not LANGCHAIN_AVAILABLE:
            print("Warning: LangChain not available. Using fallback explanations.")
            return
        
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            self.llm = ChatOpenAI(
                model=self.model_name,
                temperature=self.temperature,
                api_key=api_key
            )
            print(f"Initialized LLM: {self.model_name}")
        else:
            print("Warning: OPENAI_API_KEY not set. Using fallback explanations.")
    
    def explain_violation(
        self,
        plant_id: str,
        violation_type: str,
        value: float,
        threshold: float,
        timestamp: str,
        policy_context: str = ""
    ) -> str:
        """Explain why a violation occurred"""
        
        if self.llm and LANGCHAIN_AVAILABLE:
            prompt = PromptTemplate(
                template=VIOLATION_EXPLANATION_TEMPLATE,
                input_variables=["plant_id", "violation_type", "value", "threshold", "timestamp", "policy_context"]
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            
            result = chain.run({
                "plant_id": plant_id,
                "violation_type": violation_type,
                "value": f"{value:.1f} kg",
                "threshold": f"{threshold:.1f} kg",
                "timestamp": timestamp,
                "policy_context": policy_context or "See compliance documents for details."
            })
            return result
        else:
            # Fallback explanation
            return self._fallback_violation_explanation(plant_id, violation_type, value, threshold, timestamp)
    
    def _fallback_violation_explanation(
        self,
        plant_id: str,
        violation_type: str,
        value: float,
        threshold: float,
        timestamp: str
    ) -> str:
        """Generate fallback explanation without LLM"""
        
        exceedance = ((value - threshold) / threshold) * 100
        
        explanation = f"""🚨 Violation Explanation for {plant_id}

**Details:**
- Violation Type: {violation_type}
- Recorded Value: {value:.1f} kg CO2
- Threshold: {threshold:.1f} kg CO2
- Exceedance: {exceedance:.1f}%
- Time: {timestamp}

**Analysis:**
The facility exceeded the {violation_type} limit by {exceedance:.1f}%. This could be due to:
1. Equipment malfunction or inefficiency
2. Unexpected increase in production demand
3. Fuel quality issues
4. Maintenance backlog
5. Process optimization needed

**Recommended Actions:**
1. Investigate immediate cause of exceedance
2. Review recent operational changes
3. Check equipment performance data
4. Consider temporary production adjustment
5. Schedule comprehensive efficiency audit

**Timeline:**
- Immediate: Investigation and data review
- 24-48 hours: Preliminary findings
- 7 days: Corrective action plan
- 30 days: Implementation and verification
"""
        return explanation
    
    def summarize_carbon_activity(
        self,
        plant_data: List[Dict[str, Any]],
        time_period: str = "today"
    ) -> str:
        """Summarize carbon activity across plants"""
        
        if not plant_data:
            return "No data available for the specified period."
        
        if self.llm and LANGCHAIN_AVAILABLE:
            plant_str = json.dumps(plant_data, indent=2)
            prompt = PromptTemplate(
                template=DEFAULT_CONTEXT_TEMPLATE,
                input_variables=["context", "current_time", "question"]
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            
            result = chain.run({
                "context": f"Carbon activity data: {plant_str}",
                "current_time": datetime.now().isoformat(),
                "question": f"Summarize carbon activity for {time_period}"
            })
            return result
        else:
            return self._fallback_summary(plant_data, time_period)
    
    def _fallback_summary(self, plant_data: List[Dict], time_period: str) -> str:
        """Generate fallback summary without LLM"""
        
        total_carbon = sum(p.get('carbon_kg', 0) for p in plant_data)
        total_production = sum(p.get('production_units', 0) for p in plant_data)
        avg_efficiency = total_carbon / total_production if total_production > 0 else 0
        
        plants = [p.get('plant_id', 'Unknown') for p in plant_data]
        
        summary = f"""📊 Carbon Activity Summary ({time_period})

**Overview:**
- Total Carbon Emissions: {total_carbon:.1f} kg CO2
- Total Production: {total_production:,} units
- Average Efficiency: {avg_efficiency:.2f} kg CO2/unit

**Plants Monitored:** {', '.join(plants)}

**Status:**
- {'✅ All plants within limits' if avg_efficiency < 15 else '⚠️ Some plants need attention'}

**Recommendations:**
1. Continue real-time monitoring
2. Review efficiency trends
3. Maintain compliance documentation
"""
        return summary
    
    def compare_plants(
        self,
        plant_data: List[Dict[str, Any]]
    ) -> str:
        """Compare efficiency across plants"""
        
        if self.llm and LANGCHAIN_AVAILABLE:
            plant_str = json.dumps(plant_data, indent=2)
            prompt = PromptTemplate(
                template=EFFICIENCY_TEMPLATE,
                input_variables=["plant_data"]
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            
            result = chain.run({
                "plant_data": plant_str
            })
            return result
        else:
            return self._fallback_plant_comparison(plant_data)
    
    def _fallback_plant_comparison(self, plant_data: List[Dict]) -> str:
        """Generate fallback plant comparison"""
        
        if not plant_data:
            return "No plant data available."
        
        # Calculate efficiency for each plant
        plant_stats = []
        for p in plant_data:
            carbon = p.get('carbon_kg', 0)
            production = p.get('production_units', 0)
            efficiency = carbon / production if production > 0 else 0
            plant_stats.append({
                'plant_id': p.get('plant_id', 'Unknown'),
                'efficiency': efficiency,
                'carbon': carbon,
                'production': production
            })
        
        # Sort by efficiency
        plant_stats.sort(key=lambda x: x['efficiency'])
        
        lines = ["📊 Plant Efficiency Comparison\n"]
        lines.append("| Plant | Efficiency (kg/unit) | Carbon (kg) | Production |")
        lines.append("|-------|---------------------|-------------|------------|")
        
        for p in plant_stats:
            rating = "🟢" if p['efficiency'] < 10 else "🟡" if p['efficiency'] < 15 else "🔴"
            lines.append(f"| {rating} {p['plant_id']} | {p['efficiency']:.2f} | {p['carbon']:.1f} | {p['production']} |")
        
        # Add analysis
        best = plant_stats[0]
        worst = plant_stats[-1]
        
        lines.append(f"\n**Best Performer:** {best['plant_id']} ({best['efficiency']:.2f} kg/unit)")
        lines.append(f"**Needs Improvement:** {worst['plant_id']} ({worst['efficiency']:.2f} kg/unit)")
        
        improvement = worst['efficiency'] - best['efficiency']
        lines.append(f"\n**Potential Savings:** {improvement:.2f} kg CO2/unit if worst plant matches best.")
        
        return "\n".join(lines)
    
    def answer_question(
        self,
        question: str,
        context: str = "",
        plant_data: List[Dict] = None
    ) -> str:
        """Answer a general question about carbon compliance"""
        
        if self.llm and LANGCHAIN_AVAILABLE:
            prompt = PromptTemplate(
                template=DEFAULT_CONTEXT_TEMPLATE,
                input_variables=["context", "current_time", "question"]
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            
            context_str = context
            if plant_data:
                context_str += f"\n\nData: {json.dumps(plant_data)}"
            
            result = chain.run({
                "context": context_str,
                "current_time": datetime.now().isoformat(),
                "question": question
            })
            return result
        else:
            return self._fallback_answer(question, context, plant_data)
    
    def _fallback_answer(self, question: str, context: str, plant_data: List[Dict] = None) -> str:
        """Generate fallback answer without LLM"""
        
        question_lower = question.lower()
        
        if "why" in question_lower and "violation" in question_lower:
            return "To understand specific violation causes, please provide the plant ID and violation details."
        elif "summarize" in question_lower or "summary" in question_lower:
            return self._fallback_summary(plant_data or [], "recent period")
        elif "efficient" in question_lower or "compare" in question_lower:
            return self._fallback_plant_comparison(plant_data or [])
        elif "limit" in question_lower or "threshold" in question_lower:
            return """Carbon Compliance Limits:

**Hourly:** 500 kg CO2/hour
**Daily:** 10,000 kg CO2/day
**Efficiency:** < 15 kg CO2/production unit

See documents/carbon_policy.txt for full details."""
        else:
            return f"""I can help with:
1. Violation explanations (provide plant ID and violation type)
2. Carbon activity summaries 
3. Plant efficiency comparisons
4. Compliance limit information

Your question: "{question}"

Please provide more details for a specific answer."""


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="GreenLedger Explanation Service")
    parser.add_argument("--mode", choices=["interactive", "test"], default="interactive",
                        help="Run mode")
    parser.add_argument("--model", default="gpt-4", help="LLM model name")
    args = parser.parse_args()
    
    # Initialize service
    service = ExplanationService(model_name=args.model)
    
    if args.mode == "test":
        # Run test queries
        print("Testing Explanation Service...\n")
        
        # Test violation explanation
        print("=" * 50)
        print("Test: Violation Explanation")
        print("=" * 50)
        result = service.explain_violation(
            plant_id="Plant_A",
            violation_type="HOURLY_EMISSION_LIMIT",
            value=650.0,
            threshold=500.0,
            timestamp=datetime.now().isoformat()
        )
        print(result)
        
        # Test plant comparison
        print("\n" + "=" * 50)
        print("Test: Plant Comparison")
        print("=" * 50)
        test_data = [
            {"plant_id": "Plant_A", "carbon_kg": 450, "production_units": 50},
            {"plant_id": "Plant_B", "carbon_kg": 800, "production_units": 40},
            {"plant_id": "Plant_C", "carbon_kg": 300, "production_units": 45},
            {"plant_id": "Plant_D", "carbon_kg": 600, "production_units": 55},
        ]
        result = service.compare_plants(test_data)
        print(result)
        
    else:
        # Interactive mode
        print("GreenLedger Explanation Service")
        print("Type 'quit' to exit\n")
        
        while True:
            try:
                question = input("Ask a question: ")
                if question.lower() in ['quit', 'exit', 'q']:
                    break
                
                result = service.answer_question(question)
                print(f"\n{result}\n")
                
            except (KeyboardInterrupt, EOFError):
                break
        
        print("Goodbye!")
