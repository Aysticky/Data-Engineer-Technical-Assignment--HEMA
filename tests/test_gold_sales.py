import pytest
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from gold.generate_sales import GoldSales
from utils.config import Config


class TestGoldSales:
    
    @pytest.fixture
    def sample_silver_data(self, tmp_path):
        # Create sample silver data for testing
        data = {
            'row_id': [1, 2, 3, 4, 5],
            'order_id': ['ORD-001', 'ORD-001', 'ORD-002', 'ORD-003', 'ORD-003'],
            'order_date': pd.to_datetime(['2018-12-30', '2018-12-30', '2018-12-29', '2018-12-28', '2018-12-28']),
            'ship_date': pd.to_datetime(['2019-01-05', '2019-01-05', '2019-01-03', '2019-01-01', '2019-01-01']),
            'ship_mode': ['Standard', 'Standard', 'Express', 'Standard', 'Standard'],
            'customer_id': ['CUST-A', 'CUST-A', 'CUST-B', 'CUST-C', 'CUST-C'],
            'customer_name': ['John Doe', 'John Doe', 'Jane Smith', 'Bob Jones', 'Bob Jones'],
            'city': ['New York', 'New York', 'Chicago', 'Boston', 'Boston'],
            'segment': ['Consumer', 'Consumer', 'Corporate', 'Consumer', 'Consumer'],
            'country': ['USA', 'USA', 'USA', 'USA', 'USA']
        }
        
        df = pd.DataFrame(data)
        
        # Write to partitioned structure
        output_dir = tmp_path / "silver" / "retail_sales"
        
        for date in df['order_date'].unique():
            date_df = df[df['order_date'] == date]
            partition_dir = output_dir / f"year={date.year}" / f"month={date.month:02d}" / f"day={date.day:02d}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            date_df.to_parquet(partition_dir / "data.parquet", index=False)
        
        return str(output_dir)
    
    def test_sales_columns(self, sample_silver_data, tmp_path):
        # Test that sales dataset has exactly the required 5 columns
        generator = GoldSales(use_local=True)
        generator.input_path = sample_silver_data
        generator.output_path = str(tmp_path / "gold" / "sales")
        
        df = generator.read_silver_data(sample_silver_data)
        sales_df = generator.generate_sales_dataset(df)
        
        expected_columns = ['order_id', 'order_date', 'shipment_date', 'shipment_mode', 'city']
        assert list(sales_df.columns) == expected_columns, "Sales dataset must have exactly 5 columns in correct order"
    
    def test_sales_deduplication(self, sample_silver_data, tmp_path):
        # Test that Sales dataset deduplicates by order_id
        generator = GoldSales(use_local=True)
        generator.input_path = sample_silver_data
        generator.output_path = str(tmp_path / "gold" / "sales")
        
        df = generator.read_silver_data(sample_silver_data)
        sales_df = generator.generate_sales_dataset(df)
        
        # Should have 3 unique orders
        assert len(sales_df) == 3, "Sales dataset should deduplicate line items to order level"
        assert sales_df['order_id'].nunique() == 3, "All order_ids should be unique"
    
    def test_sales_column_rename(self, sample_silver_data, tmp_path):
        # Test that ship_date and ship_mode are renamed correctly
        generator = GoldSales(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        sales_df = generator.generate_sales_dataset(df)
        
        assert 'shipment_date' in sales_df.columns, "ship_date should be renamed to shipment_date"
        assert 'shipment_mode' in sales_df.columns, "ship_mode should be renamed to shipment_mode"
        assert 'ship_date' not in sales_df.columns, "ship_date should not exist in output"
        assert 'ship_mode' not in sales_df.columns, "ship_mode should not exist in output"
    
    def test_sales_date_types(self, sample_silver_data, tmp_path):
        # Test that dates are proper datetime64 types
        generator = GoldSales(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        sales_df = generator.generate_sales_dataset(df)
        
        assert pd.api.types.is_datetime64_any_dtype(sales_df['order_date']), "order_date must be datetime"
        assert pd.api.types.is_datetime64_any_dtype(sales_df['shipment_date']), "shipment_date must be datetime"
    
    def test_sales_no_nulls_in_key_columns(self, sample_silver_data, tmp_path):
        # Test that key columns have no nulls
        generator = GoldSales(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        sales_df = generator.generate_sales_dataset(df)
        
        assert sales_df['order_id'].notna().all(), "order_id should not have nulls"
        assert sales_df['order_date'].notna().all(), "order_date should not have nulls"
    
    def test_sales_partitioning(self, sample_silver_data, tmp_path):
        # Test that Sales data is partitioned by order_date
        generator = GoldSales(use_local=True)
        generator.input_path = sample_silver_data
        generator.output_path = str(tmp_path / "gold" / "sales")
        
        generator.run()
        
        # Check that partition directories exist
        output_path = tmp_path / "gold" / "sales"
        assert (output_path / "year=2018").exists(), "Year partition should exist"
        assert (output_path / "year=2018" / "month=12").exists(), "Month partition should exist"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
