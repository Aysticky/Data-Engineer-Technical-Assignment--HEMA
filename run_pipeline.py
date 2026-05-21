import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from orchestration.pipeline_orchestrator import PipelineOrchestrator
from utils.config import Config

if __name__ == "__main__":
    print(" Retail sales data pipeline")
    print(f"\nSource File: {Config.LOCAL_INPUT_FILE}")
    print(f"Output Directory: {Config.LOCAL_DATA_DIR}")
    
    # Run the full pipeline
    orchestrator = PipelineOrchestrator(use_local=True)
    
    try:
        metrics = orchestrator.run_full_pipeline(Config.LOCAL_INPUT_FILE)
        
        print("Summary")
        print(f"Status: {metrics['status'].upper()}")
        print(f"Total execution time: {metrics['total_execution_time_seconds']:.2f} seconds")
        print("\nLayer details:")
        print(f"Bronze: {metrics['bronze']['records_processed']} records processed")
        print(f"Silver: {metrics['silver']['records_output']} records cleansed")
        print(f"Gold Sales: {metrics['gold_sales']['records_output']} orders")
        print(f"Gold Customer: {metrics['gold_customer']['records_output']} customers")
        print("\nPipeline completed successfully!")
        print(f"\nOutput locations:")
        print(f"Bronze: {metrics['bronze']['output_path']}")
        print(f"Silver: {metrics['silver']['output_path']}")
        print(f"Gold Sales: {metrics['gold_sales']['output_path']}")
        print(f"Gold Customer: {metrics['gold_customer']['output_path']}")
        
    except Exception as e:
        print("Pipeline execution failed")
        print(f"\nError: {str(e)}")
        sys.exit(1)
