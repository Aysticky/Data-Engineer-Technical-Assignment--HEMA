import sys
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.partitioning import PartitionManager
from utils.config import Config


logger = get_logger(__name__, Config.LOG_LEVEL)


class GoldSales:
    
    def __init__(self, use_local: bool = True):
       
        self.use_local = use_local
        self.partition_manager = PartitionManager()
        logger.log_execution_start("GoldSales", use_local=use_local)
    
    def read_silver_data(self, input_path: str) -> pd.DataFrame:
        
        logger.info(f"Reading silver data from {input_path}")
        
        try:
            df = pd.read_parquet(input_path, engine='pyarrow')
            
            logger.log_data_quality(
                "silver_record_count",
                len(df),
                input_path=input_path
            )
            
            logger.info(
                "Silver data loaded successfully",
                rows=len(df),
                columns=len(df.columns)
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read silver data: {str(e)}", error=str(e))
            raise
    
    def transform_to_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        
        logger.info("Transforming to sales dataset")
        
        # Select required columns
        sales_df = df[[
            'order_id',
            'order_date',
            'ship_date',
            'ship_mode',
            'city'
        ]].copy()
        
        # Rename columns to match requirements
        sales_df = sales_df.rename(columns={
            'ship_date': 'shipment_date',
            'ship_mode': 'shipment_mode'
        })
        
        # Deduplicate at order level and keep first occurrence
        initial_count = len(sales_df)
        sales_df = sales_df.drop_duplicates(subset=['order_id'], keep='first')
        deduped_count = initial_count - len(sales_df)
        
        if deduped_count > 0:
            logger.log_data_quality(
                "orders_deduplicated",
                deduped_count
            )
        
        logger.info(
            "Sales transformation completed",
            output_records=len(sales_df)
        )
        
        return sales_df
    
    def generate_sales_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        # Alias for transform_to_sales() to match test expectations
        return self.transform_to_sales(df)
    
    def write_gold_sales(self, df: pd.DataFrame, output_path: str):
       
        logger.info(f"Writing gold sales data to {output_path}")
        
        try:
            # Add partition columns based on order_date
            df_partitioned = self.partition_manager.add_partition_columns(
                df,
                'order_date'
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
                    table="gold_sales",
                    partition=f"year={year}/month={month}/day={day}",
                    record_count=len(partition_df)
                )
            
            logger.info(
                "Gold sales data written successfully",
                total_records=len(df),
                total_partitions=len(partitions)
            )
            
        except Exception as e:
            logger.error(f"Failed to write gold sales data: {str(e)}", error=str(e))
            raise
    
    def run(self) -> Dict[str, Any]:
       
        start_time = datetime.now()
        
        try:
            # Use pre-set paths if available (for testing), otherwise use config
            if hasattr(self, 'input_path') and hasattr(self, 'output_path'):
                input_path = self.input_path
                output_path = self.output_path
            elif self.use_local:
                input_path = Config.get_local_output_path('silver')
                output_path = Config.get_local_output_path('gold', 'sales')
            else:
                input_path = Config.get_s3_path('silver')
                output_path = Config.get_s3_path('gold', 'sales')
            
            # Read silver data
            df = self.read_silver_data(input_path)
            
            # Transform to sales
            sales_df = self.transform_to_sales(df)
            
            # Write to gold layer
            self.write_gold_sales(sales_df, output_path)
            
            # Calculate metrics
            execution_time = (datetime.now() - start_time).total_seconds()
            
            metrics = {
                'status': 'success',
                'records_output': len(sales_df),
                'execution_time_seconds': execution_time,
                'output_path': output_path
            }
            
            logger.log_execution_end(
                "GoldSales",
                status="success",
                records_output=len(sales_df),
                execution_time_seconds=execution_time,
                output_path=output_path
            )
            
            return metrics
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.log_execution_end(
                "GoldSales",
                status="failed",
                error=str(e),
                execution_time_seconds=execution_time
            )
            raise


def glue_job_handler(args: List[str] = None) -> Dict[str, Any]:
   
    try:
        use_local = False
        
        # Run sales generation
        sales = GoldSales(use_local=use_local)
        metrics = sales.run()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Glue job execution failed: {str(e)}", error=str(e))
        raise


if __name__ == "__main__":
    
    print("Running gold sales generation locally")
    
    sales = GoldSales(use_local=True)
    metrics = sales.run()
    
    print(f"\nExecution completed:")
    print(f"Status: {metrics['status']}")
    print(f"Records output: {metrics['records_output']}")
    print(f"Execution time: {metrics['execution_time_seconds']:.2f} seconds")
    print(f"Output path: {metrics['output_path']}")
