import boto3
from typing import Dict, List, Optional
from datetime import datetime


class GlueCatalogManager:
    
    def __init__(self, database_name: str = "hema_retail_sales", region: str = "eu-west-1"):
        
        self.database_name = database_name
        self.region = region
        self.glue_client = boto3.client('glue', region_name=region)
    
    def create_database(self, description: str = "HEMA Retail Sales Data Lake"):
        
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': description,
                    'LocationUri': f's3://hema-retail-sales-datalake-{self.region}/',
                }
            )
            print(f"Created Glue database: {self.database_name}")
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"Glue database already exists: {self.database_name}")
        except Exception as e:
            print(f"Failed to create database: {str(e)}")
            raise
    
    def register_table(
        self,
        table_name: str,
        s3_location: str,
        columns: List[Dict[str, str]],
        partition_keys: Optional[List[Dict[str, str]]] = None,
        description: str = "",
        table_type: str = "EXTERNAL_TABLE"
    ):
        
        if partition_keys is None:
            partition_keys = [
                {'Name': 'year', 'Type': 'string'},
                {'Name': 'month', 'Type': 'string'},
                {'Name': 'day', 'Type': 'string'}
            ]
        
        table_input = {
            'Name': table_name,
            'Description': description,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {
                        'serialization.format': '1'
                    }
                },
                'StoredAsSubDirectories': False
            },
            'PartitionKeys': partition_keys,
            'TableType': table_type,
            'Parameters': {
                'classification': 'parquet',
                'compressionType': 'snappy',
                'typeOfData': 'file',
                'created_by': 'hema_etl_pipeline',
                'created_at': datetime.utcnow().isoformat()
            }
        }
        
        try:
            # Try to update existing table first
            self.glue_client.update_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
            print(f"Updated Glue table: {self.database_name}.{table_name}")
        except self.glue_client.exceptions.EntityNotFoundException:
            # Table doesn't exist, create it
            self.glue_client.create_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
            print(f"Created Glue table: {self.database_name}.{table_name}")
        except Exception as e:
            print(f"Failed to register table {table_name}: {str(e)}")
            raise
    
    def add_partitions(self, table_name: str, partitions: List[Dict]):
       
        try:
            self.glue_client.batch_create_partition(
                DatabaseName=self.database_name,
                TableName=table_name,
                PartitionInputList=partitions
            )
            print(f"Added {len(partitions)} partitions to {table_name}")
        except Exception as e:
            print(f"Failed to add partitions: {str(e)}")
    
    def register_bronze_table(self, s3_location: str):
        columns = [
            {'Name': 'row_id', 'Type': 'bigint', 'Comment': 'Unique row identifier'},
            {'Name': 'order_id', 'Type': 'string', 'Comment': 'Order identifier'},
            {'Name': 'order_date', 'Type': 'date', 'Comment': 'Order placement date'},
            {'Name': 'ship_date', 'Type': 'date', 'Comment': 'Shipment date'},
            {'Name': 'ship_mode', 'Type': 'string', 'Comment': 'Shipping mode'},
            {'Name': 'customer_id', 'Type': 'string', 'Comment': 'Customer identifier'},
            {'Name': 'customer_name', 'Type': 'string', 'Comment': 'Customer full name'},
            {'Name': 'segment', 'Type': 'string', 'Comment': 'Customer segment'},
            {'Name': 'country', 'Type': 'string', 'Comment': 'Country'},
            {'Name': 'city', 'Type': 'string', 'Comment': 'City'},
            {'Name': 'state', 'Type': 'string', 'Comment': 'State'},
            {'Name': 'postal_code', 'Type': 'string', 'Comment': 'Postal code'},
            {'Name': 'region', 'Type': 'string', 'Comment': 'Region'},
            {'Name': 'product_id', 'Type': 'string', 'Comment': 'Product identifier'},
            {'Name': 'category', 'Type': 'string', 'Comment': 'Product category'},
            {'Name': 'sub_category', 'Type': 'string', 'Comment': 'Product sub-category'},
            {'Name': 'product_name', 'Type': 'string', 'Comment': 'Product name'},
            {'Name': 'sales', 'Type': 'double', 'Comment': 'Sales amount'}
        ]
        
        self.register_table(
            table_name='bronze_retail_sales',
            s3_location=s3_location,
            columns=columns,
            description='Bronze layer - Raw retail sales data with minimal transformations'
        )
    
    def register_silver_table(self, s3_location: str):
        columns = [
            {'Name': 'row_id', 'Type': 'bigint', 'Comment': 'Unique row identifier'},
            {'Name': 'order_id', 'Type': 'string', 'Comment': 'Order identifier'},
            {'Name': 'order_date', 'Type': 'date', 'Comment': 'Order placement date'},
            {'Name': 'ship_date', 'Type': 'date', 'Comment': 'Shipment date'},
            {'Name': 'ship_mode', 'Type': 'string', 'Comment': 'Shipping mode'},
            {'Name': 'customer_id', 'Type': 'string', 'Comment': 'Customer identifier'},
            {'Name': 'customer_name', 'Type': 'string', 'Comment': 'Customer full name'},
            {'Name': 'segment', 'Type': 'string', 'Comment': 'Customer segment'},
            {'Name': 'country', 'Type': 'string', 'Comment': 'Country'},
            {'Name': 'city', 'Type': 'string', 'Comment': 'City'},
            {'Name': 'state', 'Type': 'string', 'Comment': 'State'},
            {'Name': 'postal_code', 'Type': 'string', 'Comment': 'Postal code'},
            {'Name': 'region', 'Type': 'string', 'Comment': 'Region'},
            {'Name': 'product_id', 'Type': 'string', 'Comment': 'Product identifier'},
            {'Name': 'category', 'Type': 'string', 'Comment': 'Product category'},
            {'Name': 'sub_category', 'Type': 'string', 'Comment': 'Product sub-category'},
            {'Name': 'product_name', 'Type': 'string', 'Comment': 'Product name'},
            {'Name': 'sales', 'Type': 'double', 'Comment': 'Sales amount'}
        ]
        
        self.register_table(
            table_name='silver_retail_sales',
            s3_location=s3_location,
            columns=columns,
            description='Silver layer - Cleansed and standardized retail sales data'
        )
    
    def register_gold_sales_table(self, s3_location: str):
        columns = [
            {'Name': 'order_id', 'Type': 'string', 'Comment': 'Unique order identifier'},
            {'Name': 'order_date', 'Type': 'date', 'Comment': 'Order placement date'},
            {'Name': 'shipment_date', 'Type': 'date', 'Comment': 'Shipment date'},
            {'Name': 'shipment_mode', 'Type': 'string', 'Comment': 'Shipping mode'},
            {'Name': 'city', 'Type': 'string', 'Comment': 'City'}
        ]
        
        self.register_table(
            table_name='gold_sales',
            s3_location=s3_location,
            columns=columns,
            description='Gold layer - Order-level sales dataset for analytics'
        )
    
    def register_gold_customer_table(self, s3_location: str):
        columns = [
            {'Name': 'customer_id', 'Type': 'string', 'Comment': 'Customer identifier'},
            {'Name': 'customer_first_name', 'Type': 'string', 'Comment': 'Customer first name'},
            {'Name': 'customer_last_name', 'Type': 'string', 'Comment': 'Customer last name'},
            {'Name': 'customer_segment', 'Type': 'string', 'Comment': 'Customer segment'},
            {'Name': 'country', 'Type': 'string', 'Comment': 'Country'},
            {'Name': 'orders_last_month', 'Type': 'bigint', 'Comment': 'Orders in last month from 2018-12-30'},
            {'Name': 'orders_last_6_months', 'Type': 'bigint', 'Comment': 'Orders in last 6 months from 2018-12-30'},
            {'Name': 'orders_total', 'Type': 'bigint', 'Comment': 'Total orders all time'}
        ]
        
        # Customer table partitioned by first order date
        self.register_table(
            table_name='gold_customer',
            s3_location=s3_location,
            columns=columns,
            description='Gold layer - Customer-level dataset with aggregated metrics'
        )
    
    def register_all_tables(
        self,
        bronze_path: str,
        silver_path: str,
        gold_sales_path: str,
        gold_customer_path: str
    ):
       
        print(f"\n Registering tables in Glue Data Catalog: {self.database_name}")
        
        self.create_database()
        self.register_bronze_table(bronze_path)
        self.register_silver_table(silver_path)
        self.register_gold_sales_table(gold_sales_path)
        self.register_gold_customer_table(gold_customer_path)
        
        print(f"All tables registered successfully\n")


def register_catalog_tables(use_local: bool = False):
   
    if use_local:
        print("LOCAL MODE: Skipping Glue Catalog registration")
        return
    
    from utils.config import Config
    
    catalog = GlueCatalogManager(
        database_name=Config.GLUE_DATABASE_NAME,
        region=Config.AWS_REGION
    )
    
    catalog.register_all_tables(
        bronze_path=f"s3://{Config.BRONZE_BUCKET}/{Config.BRONZE_PREFIX}/retail_sales/",
        silver_path=f"s3://{Config.SILVER_BUCKET}/{Config.SILVER_PREFIX}/retail_sales/",
        gold_sales_path=f"s3://{Config.GOLD_BUCKET}/{Config.GOLD_PREFIX}/sales/",
        gold_customer_path=f"s3://{Config.GOLD_BUCKET}/{Config.GOLD_PREFIX}/customer/"
    )
