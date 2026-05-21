import sys
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.partitioning import PartitionManager
from utils.config import Config


logger = get_logger(__name__, Config.LOG_LEVEL)


class BronzeIngestion:
        
    def __init__(self, use_local: bool = True):
        
        self.use_local = use_local
        self.partition_manager = PartitionManager()
        logger.log_execution_start("BronzeIngestion", use_local=use_local)
    
    def read_source_data(self, file_path: str) -> pd.DataFrame:
        
        logger.info(f"Reading source data from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            logger.log_data_quality(
                "source_record_count", 
                len(df),
                file_path=file_path
            )
            
            logger.info(
                "Source data loaded successfully",
                rows=len(df),
                columns=len(df.columns)
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read source data: {str(e)}", error=str(e))
            raise
    
    def write_bronze_data(self, df: pd.DataFrame, output_path: str):
       
        logger.info(f"Writing bronze data to {output_path}")
        
        try:
           
            df['Order Date'] = pd.to_datetime(df['Order Date'], format='mixed', dayfirst=True)
            
            # Add partition columns based on Order Date
            df_partitioned = self.partition_manager.add_partition_columns(
                df, 
                'Order Date'
            )
            
            # Get unique partitions
            partitions = df_partitioned[['year', 'month', 'day']].drop_duplicates()
            
            logger.info(
                f"Writing {len(partitions)} partitions",
                partition_count=len(partitions)
            )
            
            # Write each partition
            for _, partition_vals in partitions.iterrows():
                year = partition_vals['year']
                month = partition_vals['month']
                day = partition_vals['day']
                
                # Filter data for this partition
                partition_df = df_partitioned[
                    (df_partitioned['year'] == year) &
                    (df_partitioned['month'] == month) &
                    (df_partitioned['day'] == day)
                ].copy()
                
                # Remove partition columns from data
                partition_df = partition_df.drop(columns=['year', 'month', 'day'])
                
                # Generate partition path
                partition_path = f"{output_path}/year={year}/month={month}/day={day}"
                
                # Create directory if local
                if self.use_local:
                    os.makedirs(partition_path, exist_ok=True)
                    file_path = f"{partition_path}/data.parquet"
                else:
                    file_path = f"{partition_path}/data.parquet"
                
                # Write parquet file
                partition_df.to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression=Config.COMPRESSION,
                    index=False
                )
                
                logger.log_partition_write(
                    table="bronze_retail_sales",
                    partition=f"year={year}/month={month}/day={day}",
                    record_count=len(partition_df)
                )
            
            logger.info(
                "Bronze data written successfully",
                total_records=len(df),
                total_partitions=len(partitions)
            )
            
        except Exception as e:
            logger.error(f"Failed to write bronze data: {str(e)}", error=str(e))
            raise
    
    def run(self, source_file: str) -> Dict[str, Any]:
        
        start_time = datetime.now()
        
        try:
            df = self.read_source_data(source_file)
            
            if self.use_local:
                output_path = Config.get_local_output_path('bronze')
            else:
                output_path = Config.get_s3_path('bronze')
            
            # Write to bronze layer
            self.write_bronze_data(df, output_path)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            metrics = {
                'status': 'success',
                'records_processed': len(df),
                'execution_time_seconds': execution_time,
                'output_path': output_path
            }
            
            logger.log_execution_end(
                "BronzeIngestion",
                status="success",
                records_processed=len(df),
                execution_time_seconds=execution_time,
                output_path=output_path
            )
            
            return metrics
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.log_execution_end(
                "BronzeIngestion",
                status="failed",
                error=str(e),
                execution_time_seconds=execution_time
            )
            raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
   
    try:
        # Extract parameters from event
        source_file = event.get('source_file', Config.LOCAL_INPUT_FILE)
        use_local = event.get('use_local', False)
        
        # Run ingestion
        ingestion = BronzeIngestion(use_local=use_local)
        metrics = ingestion.run(source_file)
        
        return {
            'statusCode': 200,
            'body': metrics
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", error=str(e))
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }


if __name__ == "__main__":
    
    print("Running bronze ingestion locally")
    
    ingestion = BronzeIngestion(use_local=True)
    metrics = ingestion.run(Config.LOCAL_INPUT_FILE)
    
    print(f"\nExecution completed:")
    print(f"Status: {metrics['status']}")
    print(f"Records processed: {metrics['records_processed']}")
    print(f"Execution time: {metrics['execution_time_seconds']:.2f} seconds")
    print(f"Output path: {metrics['output_path']}")
