import sys
import os
from datetime import datetime
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.config import Config


logger = get_logger(__name__, Config.LOG_LEVEL)


class PipelineOrchestrator:
    
    def __init__(self, use_local: bool = True):
        
        self.use_local = use_local
        logger.log_execution_start("PipelineOrchestrator", use_local=use_local)
    
    def run_bronze_layer(self, source_file: str) -> Dict[str, Any]:
        
        logger.info("Starting bronze layer execution")
        
        try:
            from bronze.ingest_raw_data import BronzeIngestion
            
            bronze = BronzeIngestion(use_local=self.use_local)
            metrics = bronze.run(source_file)
            
            logger.info(
                "Bronze layer completed successfully",
                metrics=metrics
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Bronze layer failed: {str(e)}", error=str(e))
            raise
    
    def run_silver_layer(self) -> Dict[str, Any]:
       
        logger.info("Starting silver layer execution")
        
        try:
            from silver.cleanse_data import SilverCleansing
            
            silver = SilverCleansing(use_local=self.use_local)
            metrics = silver.run()
            
            logger.info(
                "Silver layer completed successfully",
                metrics=metrics
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Silver layer failed: {str(e)}", error=str(e))
            raise
    
    def run_gold_sales_layer(self) -> Dict[str, Any]:
        
        logger.info("Starting gold sales layer execution")
        
        try:
            from gold.generate_sales import GoldSales
            
            gold_sales = GoldSales(use_local=self.use_local)
            metrics = gold_sales.run()
            
            logger.info(
                "Gold sales layer completed successfully",
                metrics=metrics
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Gold sales layer failed: {str(e)}", error=str(e))
            raise
    
    def run_gold_customer_layer(self) -> Dict[str, Any]:
       
        logger.info("Starting gold customer layer execution")
        
        try:
            from gold.generate_customer import GoldCustomer
            
            gold_customer = GoldCustomer(use_local=self.use_local)
            metrics = gold_customer.run()
            
            logger.info(
                "Gold customer layer completed successfully",
                metrics=metrics
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Gold vustomer layer failed: {str(e)}", error=str(e))
            raise
    
    def run_full_pipeline(self, source_file: str) -> Dict[str, Any]:
       
        start_time = datetime.now()
        
        logger.info(
            "Starting full pipeline execution",
            source_file=source_file
        )
        
        try:
            # Bronze kayer
            bronze_metrics = self.run_bronze_layer(source_file)
            
            # Silver layer
            silver_metrics = self.run_silver_layer()
            
            # Gold layer sales
            gold_sales_metrics = self.run_gold_sales_layer()
            
            # Gold layer customer
            gold_customer_metrics = self.run_gold_customer_layer()
            
            # Overall metrics
            execution_time = (datetime.now() - start_time).total_seconds()
            
            overall_metrics = {
                'status': 'success',
                'total_execution_time_seconds': execution_time,
                'bronze': bronze_metrics,
                'silver': silver_metrics,
                'gold_sales': gold_sales_metrics,
                'gold_customer': gold_customer_metrics
            }
            
            logger.log_execution_end(
                "PipelineOrchestrator",
                status="success",
                execution_time_seconds=execution_time
            )
            
            logger.info(
                "Full pipeline completed successfully",
                total_execution_time=execution_time
            )
            
            return overall_metrics
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.log_execution_end(
                "PipelineOrchestrator",
                status="failed",
                error=str(e),
                execution_time_seconds=execution_time
            )
            
            raise


def step_functions_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
   
    try:
        source_file = event.get('source_file', Config.LOCAL_INPUT_FILE)
        use_local = event.get('use_local', False)
        
        orchestrator = PipelineOrchestrator(use_local=use_local)
        metrics = orchestrator.run_full_pipeline(source_file)
        
        return {
            'statusCode': 200,
            'body': metrics
        }
        
    except Exception as e:
        logger.error(f"Step Functions execution failed: {str(e)}", error=str(e))
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }


if __name__ == "__main__":
   
    print("retail sales data pipeline")
    
    orchestrator = PipelineOrchestrator(use_local=True)
    metrics = orchestrator.run_full_pipeline(Config.LOCAL_INPUT_FILE)
    
    print("Summary")
    print(f"Status: {metrics['status']}")
    print(f"Total execution time: {metrics['total_execution_time_seconds']:.2f} seconds")
    print("\nLayer details:")
    print(f"Bronze: {metrics['bronze']['records_processed']} records processed")
    print(f"Silver: {metrics['silver']['records_output']} records output")
    print(f"Gold Sales: {metrics['gold_sales']['records_output']} records output")
    print(f"Gold Customer: {metrics['gold_customer']['records_output']} records output")
