
import subprocess
import sys
import os
import argparse
from pathlib import Path


def install_dependencies():
    """Install required dependencies"""
    print("Installing dependencies...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])


def run_simulator(duration: int = None, interval: float = 1.0):
    """Run the data simulator"""
    print("Starting data simulator...")
    cmd = [sys.executable, "data_simulator.py"]
    if duration:
        cmd.extend(["--duration", str(duration)])
    if interval:
        cmd.extend(["--interval", str(interval)])
    
    subprocess.check_call(cmd)


def run_carbon_pipeline():
    """Run the carbon calculation pipeline"""
    print("Starting carbon pipeline...")
    cmd = [sys.executable, "carbon_pipeline.py"]
    subprocess.check_call(cmd)


def run_anomaly_detector():
    """Run the anomaly detector"""
    print("Starting anomaly detector...")
    cmd = [sys.executable, "anomaly_detector.py"]
    subprocess.check_call(cmd)


def run_compliance_engine():
    """Run the compliance engine"""
    print("Starting compliance engine...")
    cmd = [sys.executable, "compliance_engine.py"]
    subprocess.check_call(cmd)


def run_rag_engine(query: str = None):
    """Run the RAG engine"""
    if query:
        print(f"Running RAG query: {query}")
        cmd = [sys.executable, "rag_engine.py", "--query", query]
    else:
        print("Starting RAG engine...")
        cmd = [sys.executable, "rag_engine.py"]
    subprocess.check_call(cmd)


def run_explanation_service(mode: str = "test"):
    """Run the explanation service"""
    print(f"Starting explanation service in {mode} mode...")
    cmd = [sys.executable, "explanation_service.py", "--mode", mode]
    subprocess.check_call(cmd)


def run_dashboard():
    """Run the Streamlit dashboard"""
    print("Starting dashboard...")
    cmd = ["streamlit", "run", "dashboard.py"]
    subprocess.check_call(cmd)


def run_demo():
    """Run a complete demo of the system"""
    print("=" * 60)
    print("GreenLedger Demo")
    print("=" * 60)
    
    # Start dashboard in background
    print("\nStarting dashboard...")
    dashboard_proc = subprocess.Popen(
        ["streamlit", "run", "dashboard.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        # Wait for user to stop
        print("\nDashboard started! Press Ctrl+C to stop.")
        print("Open http://localhost:8501 in your browser")
        
        # Keep running
        while True:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping dashboard...")
        dashboard_proc.terminate()
        dashboard_proc.wait()


def main():
    parser = argparse.ArgumentParser(description="GreenLedger Runner")
    parser.add_argument("command", choices=[
        "install",
        "simulator",
        "pipeline",
        "anomaly",
        "compliance",
        "rag",
        "explain",
        "dashboard",
        "demo"
    ], help="Command to run")
    parser.add_argument("--duration", type=int, help="Duration for simulator (seconds)")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval for simulator")
    parser.add_argument("--query", type=str, help="Query for RAG engine")
    parser.add_argument("--mode", choices=["test", "interactive"], default="test", 
                       help="Mode for explanation service")
    
    args = parser.parse_args()
    
    # Change to script directory
    os.chdir(Path(__file__).parent)
    
    if args.command == "install":
        install_dependencies()
    elif args.command == "simulator":
        run_simulator(args.duration, args.interval)
    elif args.command == "pipeline":
        run_carbon_pipeline()
    elif args.command == "anomaly":
        run_anomaly_detector()
    elif args.command == "compliance":
        run_compliance_engine()
    elif args.command == "rag":
        run_rag_engine(args.query)
    elif args.command == "explain":
        run_explanation_service(args.mode)
    elif args.command == "dashboard":
        run_dashboard()
    elif args.command == "demo":
        run_demo()


if __name__ == "__main__":
    main()
