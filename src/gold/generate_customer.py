import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.partitioning import PartitionManager
from utils.config import Config


logger = get_logger(__name__, Config.LOG_LEVEL)


class GoldCustomer:
    
    def __init__(self, use_local: bool = True):
       
        self.use_local = use_local
        self.partition_manager = PartitionManager()
        self.dataset_latest_date = pd.to_datetime(Config.DATASET_LATEST_DATE)
        logger.log_execution_start("GoldCustomer", use_local=use_local)
    
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
    
    def parse_customer_name(self, df: pd.DataFrame) -> pd.DataFrame:
        
        logger.info("Parsing customer names")
        
        df = df.copy()
        
        # Split customer name on first space
        name_parts = df['customer_name'].str.split(' ', n=1, expand=True)
        
        df['customer_first_name'] = name_parts[0]
        df['customer_last_name'] = name_parts[1] if len(name_parts.columns) > 1 else ''
        
        # Fill nulls with empty string
        df['customer_last_name'] = df['customer_last_name'].fillna('')
        
        return df
    
    def calculate_order_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
       
        logger.info("Calculating order metrics")
        
        latest_date = self.dataset_latest_date
        one_month_ago = latest_date - pd.DateOffset(months=1)
        six_months_ago = latest_date - pd.DateOffset(months=6)
        
        logger.info(
            "Date thresholds calculated",
            latest_date=str(latest_date),
            one_month_ago=str(one_month_ago),
            six_months_ago=str(six_months_ago)
        )
        
        # Parse customer names first
        df = self.parse_customer_name(df)
        
        # Group by customer and calculate metrics
        customer_groups = df.groupby('customer_id')
        
        # All-time order count
        orders_total = customer_groups['order_id'].nunique().reset_index()
        orders_total.columns = ['customer_id', 'orders_total']
        
        # Last month order count
        df_last_month = df[df['order_date'] >= one_month_ago]
        orders_last_month = df_last_month.groupby('customer_id')['order_id'].nunique().reset_index()
        orders_last_month.columns = ['customer_id', 'orders_last_month']
        
        # Last 6 months order count
        df_last_6_months = df[df['order_date'] >= six_months_ago]
        orders_last_6_months = df_last_6_months.groupby('customer_id')['order_id'].nunique().reset_index()
        orders_last_6_months.columns = ['customer_id', 'orders_last_6_months']
        
        # Get one record per customer with their attributes
        customer_attrs = df.groupby('customer_id').agg({
            'customer_first_name': 'first',
            'customer_last_name': 'first',
            'segment': 'first',
            'country': 'first'
        }).reset_index()
        
        # Merge all metrics
        customer_df = customer_attrs.merge(orders_total, on='customer_id', how='left')
        customer_df = customer_df.merge(orders_last_month, on='customer_id', how='left')
        customer_df = customer_df.merge(orders_last_6_months, on='customer_id', how='left')
        
        # Fill nulls with 0 for order counts
        customer_df['orders_last_month'] = customer_df['orders_last_month'].fillna(0).astype(int)
        customer_df['orders_last_6_months'] = customer_df['orders_last_6_months'].fillna(0).astype(int)
        
        # Rename segment column
        customer_df = customer_df.rename(columns={'segment': 'customer_segment'})
        
        # Select final columns in the correct order
        customer_df = customer_df[[
            'customer_id',
            'customer_first_name',
            'customer_last_name',
            'customer_segment',
            'country',
            'orders_last_month',
            'orders_last_6_months',
            'orders_total'
        ]]
        
        logger.info(
            "Order metrics calculated",
            unique_customers=len(customer_df)
        )
        
        logger.log_data_quality(
            "unique_customers",
            len(customer_df)
        )
        
        return customer_df
    
    def transform_to_customer(self, df: pd.DataFrame) -> pd.DataFrame:
       
        logger.info("transforming to customer dataset")
        
        # Calculate customer metrics
        customer_df = self.calculate_order_metrics(df)
        
        # Get the latest order date for each customer for partitioning
        latest_order_dates = df.groupby('customer_id')['order_date'].max().reset_index()
        
        # Merge with customer data
        customer_df = customer_df.merge(latest_order_dates, on='customer_id', how='left')
        
        logger.info(
            "Customer transformation completed",
            output_records=len(customer_df)
        )
        
        return customer_df
    
    def generate_customer_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        # Alias for calculate_order_metrics() to match test expectations
        return self.calculate_order_metrics(df)
    
    def write_gold_customer(self, df: pd.DataFrame, output_path: str):
        
        logger.info(f"Writing gold customer data to {output_path}")
        
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
                
                # Remove partition columns and order_date from output
                partition_df = partition_df.drop(columns=['year', 'month', 'day', 'order_date'])
                
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
                    table="gold_customer",
                    partition=f"year={year}/month={month}/day={day}",
                    record_count=len(partition_df)
                )
            
            logger.info(
                "Gold customer data written successfully",
                total_records=len(df),
                total_partitions=len(partitions)
            )
            
        except Exception as e:
            logger.error(f"Failed to write gold customer data: {str(e)}", error=str(e))
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
                output_path = Config.get_local_output_path('gold', 'customer')
            else:
                input_path = Config.get_s3_path('silver')
                output_path = Config.get_s3_path('gold', 'customer')
            
            # Read silver data
            df = self.read_silver_data(input_path)
            
            # Transform to customer
            customer_df = self.transform_to_customer(df)
            
            # Write to gold layer
            self.write_gold_customer(customer_df, output_path)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            metrics = {
                'status': 'success',
                'records_output': len(customer_df),
                'execution_time_seconds': execution_time,
                'output_path': output_path
            }
            
            logger.log_execution_end(
                "GoldCustomer",
                status="success",
                records_output=len(customer_df),
                execution_time_seconds=execution_time,
                output_path=output_path
            )
            
            return metrics
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.log_execution_end(
                "GoldCustomer",
                status="failed",
                error=str(e),
                execution_time_seconds=execution_time
            )
            raise


def glue_job_handler(args: List[str] = None) -> Dict[str, Any]:
   
    try:
        use_local = False
        
        # Run customer generation
        customer = GoldCustomer(use_local=use_local)
        metrics = customer.run()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Glue job execution failed: {str(e)}", error=str(e))
        raise


if __name__ == "__main__":
   
    print("Running gold customer generation locally")
    
    customer = GoldCustomer(use_local=True)
    metrics = customer.run()
    
    print(f"\nExecution completed:")
    print(f"Status: {metrics['status']}")
    print(f"Records output: {metrics['records_output']}")
    print(f"Execution time: {metrics['execution_time_seconds']:.2f} seconds")
    print(f"Output path: {metrics['output_path']}")
