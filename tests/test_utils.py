import pytest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.partitioning import PartitionManager
from utils.logger import get_logger
from utils.config import Config


class TestPartitionManager:
    
    def test_get_partition_path(self):
        date = datetime(2018, 12, 30)
        base_path = "s3://bucket/layer"
        
        path = PartitionManager.get_partition_path(date, base_path, "table")
        
        assert "year=2018" in path
        assert "month=12" in path
        assert "day=30" in path
        assert path.startswith("s3://bucket/layer/table")
    
    def test_get_partition_columns(self):
        # Test partition column list
        columns = PartitionManager.get_partition_columns()
        
        assert columns == ['year', 'month', 'day']
    
    def test_add_partition_columns(self):
        # Test adding partition columns to dataframe
        df = pd.DataFrame({
            'order_date': pd.to_datetime(['2018-12-01', '2018-12-15', '2018-12-30']),
            'value': [1, 2, 3]
        })
        
        df_partitioned = PartitionManager.add_partition_columns(df, 'order_date')
        
        assert 'year' in df_partitioned.columns
        assert 'month' in df_partitioned.columns
        assert 'day' in df_partitioned.columns
        assert df_partitioned.iloc[0]['year'] == 2018
        assert df_partitioned.iloc[0]['month'] == '12'
        assert df_partitioned.iloc[0]['day'] == '01'
    
    def test_get_partition_filter(self):
        # Test partition filter generation
        filters = PartitionManager.get_partition_filter(year=2018, month=12, day=30)
        
        assert filters['year'] == '2018'
        assert filters['month'] == '12'
        assert filters['day'] == '30'
    
    def test_generate_partition_predicate(self):
        # Test SQL partition predicate generation
        predicate = PartitionManager.generate_partition_predicate(year=2018, month=12)
        
        assert 'year=2018' in predicate
        assert 'month=12' in predicate
        assert 'AND' in predicate


class TestLogger:
    # Test logging functionality
    
    def test_logger_initialization(self):
        logger = get_logger("test", "INFO")
        
        assert logger is not None
        assert logger.logger.name == "test"
    
    def test_logger_levels(self):
        # Test different log levels
        logger = get_logger("test", "DEBUG")
        
        # Should not raise exceptions
        logger.debug("Debug message", test_field="value")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")


class TestConfig:
    
    def test_get_s3_path_bronze(self):
        # Test S3 path generation for bronze laye
        path = Config.get_s3_path('bronze')
        
        assert path.startswith('s3://')
        assert 'bronze' in path.lower()
    
    def test_get_s3_path_silver(self):
        # Test S3 path generation for silver layer
        path = Config.get_s3_path('silver')
        
        assert path.startswith('s3://')
        assert 'silver' in path.lower()
    
    def test_get_s3_path_gold_sales(self):
        # Test S3 path generation for gold sales
        path = Config.get_s3_path('gold', 'sales')
        
        assert path.startswith('s3://')
        assert 'gold' in path.lower()
        assert 'sales' in path.lower()
    
    def test_get_s3_path_gold_customer(self):
        # Test S3 path generation for gold customer
        path = Config.get_s3_path('gold', 'customer')
        
        assert path.startswith('s3://')
        assert 'gold' in path.lower()
        assert 'customer' in path.lower()
    
    def test_invalid_layer_raises_error(self):
        # Test that invalid layer raises ValueError
        with pytest.raises(ValueError):
            Config.get_s3_path('invalid')
    
    def test_invalid_gold_table_raises_error(self):
        # Test that invalid gold table raises ValueError
        with pytest.raises(ValueError):
            Config.get_s3_path('gold', 'invalid')
