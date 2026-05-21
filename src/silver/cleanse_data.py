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


class SilverCleansing:
       
    def __init__(self, use_local: bool = True):
        
        self.use_local = use_local
        self.partition_manager = PartitionManager()
        logger.log_execution_start("SilverCleansing", use_local=use_local)
    
    def read_bronze_data(self, input_path: str) -> pd.DataFrame:
        
        logger.info(f"Reading bronze data from {input_path}")
        
        try:
            df = pd.read_parquet(input_path, engine='pyarrow')
            
            logger.log_data_quality(
                "bronze_record_count",
                len(df),
                input_path=input_path
            )
            
            logger.info(
                "Bronze data loaded successfully",
                rows=len(df),
                columns=len(df.columns)
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read bronze data: {str(e)}", error=str(e))
            raise
    
    def validate_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        
        required_columns = [
            'Row ID', 'Order ID', 'Order Date', 'Ship Date', 'Ship Mode',
            'Customer ID', 'Customer Name', 'Segment', 'Country', 'City'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("All required columns present")
        return df
    
    def cleanse_data(self, df: pd.DataFrame) -> pd.DataFrame:
       
        logger.info("Starting data cleansing")
        
        df_clean = df.copy()
        initial_count = len(df_clean)
        
        # 1. Remove duplicates based on Row ID
        df_clean = df_clean.drop_duplicates(subset=['Row ID'], keep='first')
        duplicates_removed = initial_count - len(df_clean)
        
        if duplicates_removed > 0:
            logger.log_data_quality(
                "duplicates_removed",
                duplicates_removed
            )
        
        # 2. Convert date columns to datetime with proper format
        df_clean['Order Date'] = pd.to_datetime(
            df_clean['Order Date'], 
            format='mixed',
            dayfirst=True,
            errors='coerce'
        )
        df_clean['Ship Date'] = pd.to_datetime(
            df_clean['Ship Date'],
            format='mixed',
            dayfirst=True,
            errors='coerce'
        )
        
        # 3. Check for invalid dates
        invalid_order_dates = df_clean['Order Date'].isna().sum()
        invalid_ship_dates = df_clean['Ship Date'].isna().sum()
        
        if invalid_order_dates > 0:
            logger.log_data_quality(
                "invalid_order_dates",
                invalid_order_dates
            )
        
        if invalid_ship_dates > 0:
            logger.log_data_quality(
                "invalid_ship_dates",
                invalid_ship_dates
            )
        
        # 4. Remove records with null order date
        df_clean = df_clean[df_clean['Order Date'].notna()]
        null_dates_removed = len(df) - duplicates_removed - len(df_clean)
        
        if null_dates_removed > 0:
            logger.log_data_quality(
                "null_order_dates_removed",
                null_dates_removed
            )
        
        # 5. Standardize text fields
        text_columns = ['Customer Name', 'Segment', 'Country', 'City', 'Ship Mode']
        for col in text_columns:
            df_clean[col] = df_clean[col].str.strip()
        
        # 6. Validate ship date to be greater or equals order date
        invalid_ship_dates = (df_clean['Ship Date'] < df_clean['Order Date']).sum()
        if invalid_ship_dates > 0:
            logger.log_data_quality(
                "invalid_ship_dates_before_order",
                invalid_ship_dates
            )
            # Set invalid ship dates to null
            df_clean.loc[df_clean['Ship Date'] < df_clean['Order Date'], 'Ship Date'] = pd.NaT
        
        # 7. Ensure customer ID and order ID are strings
        df_clean['Customer ID'] = df_clean['Customer ID'].astype(str)
        df_clean['Order ID'] = df_clean['Order ID'].astype(str)
        df_clean['Row ID'] = df_clean['Row ID'].astype(int)
        
        logger.info(
            "Data cleansing completed",
            initial_records=initial_count,
            final_records=len(df_clean),
            records_removed=initial_count - len(df_clean)
        )
        
        return df_clean
    
    def standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        
        logger.info("Standardizing schema")
        
        df_std = df.copy()
    
        column_mapping = {
            'Row ID': 'row_id',
            'Order ID': 'order_id',
            'Order Date': 'order_date',
            'Ship Date': 'ship_date',
            'Ship Mode': 'ship_mode',
            'Customer ID': 'customer_id',
            'Customer Name': 'customer_name',
            'Segment': 'segment',
            'Country': 'country',
            'City': 'city'
        }
        
        df_std = df_std.rename(columns=column_mapping)
        
        logger.info("Schema standardization completed")
        
        return df_std
    
    def write_silver_data(self, df: pd.DataFrame, output_path: str):
        
        logger.info(f"Writing silver data to {output_path}")
        
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
                    table="silver_retail_sales",
                    partition=f"year={year}/month={month}/day={day}",
                    record_count=len(partition_df)
                )
            
            logger.info(
                "Silver data written successfully",
                total_records=len(df),
                total_partitions=len(partitions)
            )
            
        except Exception as e:
            logger.error(f"Failed to write silver data: {str(e)}", error=str(e))
            raise
    
    def run(self) -> Dict[str, Any]:
        
        start_time = datetime.now()
        
        try:
            if self.use_local:
                input_path = Config.get_local_output_path('bronze')
                output_path = Config.get_local_output_path('silver')
            else:
                input_path = Config.get_s3_path('bronze')
                output_path = Config.get_s3_path('silver')
            
            # Read bronze data
            df = self.read_bronze_data(input_path)
            
            # Validate columns
            df = self.validate_required_columns(df)
            
            # Cleanse data
            df_clean = self.cleanse_data(df)
            
            # Standardize schema
            df_std = self.standardize_schema(df_clean)
            
            # Write to silver layer
            self.write_silver_data(df_std, output_path)
            
            # Calculate metrics
            execution_time = (datetime.now() - start_time).total_seconds()
            
            metrics = {
                'status': 'success',
                'records_input': len(df),
                'records_output': len(df_std),
                'execution_time_seconds': execution_time,
                'output_path': output_path
            }
            
            logger.log_execution_end(
                "SilverCleansing",
                status="success",
                records_input=len(df),
                records_output=len(df_std),
                execution_time_seconds=execution_time,
                output_path=output_path
            )
            
            return metrics
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.log_execution_end(
                "SilverCleansing",
                status="failed",
                error=str(e),
                execution_time_seconds=execution_time
            )
            raise


def glue_job_handler(args: List[str] = None) -> Dict[str, Any]:
   
    try:
        use_local = False
        
        # Run cleansing
        cleansing = SilverCleansing(use_local=use_local)
        metrics = cleansing.run()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Glue job execution failed: {str(e)}", error=str(e))
        raise


if __name__ == "__main__":
   
    print("Running silver cleansing locally...")
    
    cleansing = SilverCleansing(use_local=True)
    metrics = cleansing.run()
    
    print(f"\nExecution completed:")
    print(f"Status: {metrics['status']}")
    print(f"Records input: {metrics['records_input']}")
    print(f"Records output: {metrics['records_output']}")
    print(f"Execution time: {metrics['execution_time_seconds']:.2f} seconds")
    print(f"Output path: {metrics['output_path']}")
