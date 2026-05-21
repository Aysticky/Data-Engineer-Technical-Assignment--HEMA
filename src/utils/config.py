import os
from typing import Optional


class Config:
    # AWS Configuration
    AWS_REGION: str = os.getenv('AWS_REGION', 'eu-west-1')
    
    # S3 Bucket Configuration
    BRONZE_BUCKET: str = os.getenv('BRONZE_BUCKET', 'hema-data-bronze')
    SILVER_BUCKET: str = os.getenv('SILVER_BUCKET', 'hema-data-silver')
    GOLD_BUCKET: str = os.getenv('GOLD_BUCKET', 'hema-data-gold')
    
    # S3 Paths
    BRONZE_PREFIX: str = 'retail_sales'
    SILVER_PREFIX: str = 'retail_sales'
    GOLD_PREFIX: str = 'gold'  # Base gold prefix
    GOLD_SALES_PREFIX: str = 'sales'
    GOLD_CUSTOMER_PREFIX: str = 'customer'
    
    # Glue Data Catalog Configuration
    GLUE_DATABASE_NAME: str = os.getenv('GLUE_DATABASE', 'hema_retail_sales')
    
    # Legacy: Individual database names
    BRONZE_DATABASE: str = 'hema_retail_bronze'
    SILVER_DATABASE: str = 'hema_retail_silver'
    GOLD_DATABASE: str = 'hema_retail_gold'
    
    # Glue Table Names
    BRONZE_TABLE: str = 'retail_sales_raw'
    SILVER_TABLE: str = 'retail_sales_cleansed'
    GOLD_SALES_TABLE: str = 'sales'
    GOLD_CUSTOMER_TABLE: str = 'customer'
    
    # Local Data Paths
    LOCAL_DATA_DIR: str = os.getenv('LOCAL_DATA_DIR', 'data')
    LOCAL_INPUT_FILE: str = os.getenv('LOCAL_INPUT_FILE', 'data/train.csv')
    
    # Logging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Date Configuration (for customer aggregations)
    DATASET_LATEST_DATE: str = '2018-12-30'  # Latest date in the dataset
    
    # File Format
    FILE_FORMAT: str = 'parquet'
    COMPRESSION: str = 'snappy'
    
    @classmethod
    def get_s3_path(cls, layer: str, table: Optional[str] = None) -> str:
       
        if layer == 'bronze':
            return f"s3://{cls.BRONZE_BUCKET}/{cls.BRONZE_PREFIX}"
        elif layer == 'silver':
            return f"s3://{cls.SILVER_BUCKET}/{cls.SILVER_PREFIX}"
        elif layer == 'gold':
            if table == 'sales':
                return f"s3://{cls.GOLD_BUCKET}/{cls.GOLD_SALES_PREFIX}"
            elif table == 'customer':
                return f"s3://{cls.GOLD_BUCKET}/{cls.GOLD_CUSTOMER_PREFIX}"
            else:
                raise ValueError(f"Unknown gold table: {table}")
        else:
            raise ValueError(f"Unknown layer: {layer}")
    
    @classmethod
    def get_local_output_path(cls, layer: str, table: Optional[str] = None) -> str:
        
        base = cls.LOCAL_DATA_DIR
        
        if layer == 'bronze':
            return f"{base}/bronze/{cls.BRONZE_PREFIX}"
        elif layer == 'silver':
            return f"{base}/silver/{cls.SILVER_PREFIX}"
        elif layer == 'gold':
            if table == 'sales':
                return f"{base}/gold/{cls.GOLD_SALES_PREFIX}"
            elif table == 'customer':
                return f"{base}/gold/{cls.GOLD_CUSTOMER_PREFIX}"
            else:
                raise ValueError(f"Unknown gold table: {table}")
        else:
            raise ValueError(f"Unknown layer: {layer}")
