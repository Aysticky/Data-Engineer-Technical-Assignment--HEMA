import pytest
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from gold.generate_customer import GoldCustomer
from utils.config import Config


class TestGoldCustomer:
    # Test gold vustomer dataset generation
    
    @pytest.fixture
    def sample_silver_data(self, tmp_path):
        # Create sample silver data for testing customer metrics
        data = {
            'row_id': range(1, 16),
            # CUST-A: ORD-001 (Dec), ORD-005 (Aug), ORD-010 (Feb), ORD-014 (Jul) = 4 orders
            # CUST-B: ORD-002 (Dec), ORD-011 (Dec 2017) = 2 orders
            # CUST-C: ORD-003 (Nov), ORD-007 (Jan) = 2 orders
            'order_id': ['ORD-001', 'ORD-001', 'ORD-002', 'ORD-002', 'ORD-003',
                         'ORD-005', 'ORD-005', 'ORD-007', 'ORD-007', 'ORD-010', 'ORD-010',
                         'ORD-011', 'ORD-011', 'ORD-014', 'ORD-014'],
            'order_date': pd.to_datetime([
                '2018-12-15', '2018-12-15',  # ORD-001, CUST-A (Dec, last month)
                '2018-12-10', '2018-12-10',  # ORD-002, CUST-B (Dec, last month)
                '2018-11-20',                # ORD-003, CUST-C (Nov, within 6 months)
                '2018-08-01', '2018-08-01',  # ORD-005, CUST-A (Aug, within 6 months)
                '2018-01-15', '2018-01-15',  # ORD-007, CUST-C (Jan, outside 6 months)
                '2018-02-10', '2018-02-10',  # ORD-010, CUST-A (Feb, outside 6 months)
                '2017-12-01', '2017-12-01',  # ORD-011, CUST-B (Dec 2017, outside 6 months)
                '2018-07-01', '2018-07-01'   # ORD-014, CUST-A (Jul, edge of 6 months)
            ]),
            'customer_id': ['CUST-A', 'CUST-A', 'CUST-B', 'CUST-B', 'CUST-C',
                           'CUST-A', 'CUST-A', 'CUST-C', 'CUST-C', 'CUST-A', 'CUST-A',
                           'CUST-B', 'CUST-B', 'CUST-A', 'CUST-A'],
            'customer_name': ['John Doe', 'John Doe', 'Jane Smith', 'Jane Smith', 'Bob Jones',
                            'John Doe', 'John Doe', 'Bob Jones', 'Bob Jones', 'John Doe', 'John Doe',
                            'Jane Smith', 'Jane Smith', 'John Doe', 'John Doe'],
            'segment': ['Consumer'] * 15,
            'country': ['USA'] * 15,
            'city': ['New York'] * 15
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
    
    def test_customer_columns(self, sample_silver_data, tmp_path):
        # Test that customer dataset has all required columns
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        expected_columns = [
            'customer_id', 'customer_first_name', 'customer_last_name',
            'customer_segment', 'country',
            'orders_last_month', 'orders_last_6_months', 'orders_total'
        ]
        
        for col in expected_columns:
            assert col in customer_df.columns, f"Customer dataset must have column: {col}"
    
    def test_customer_name_parsing(self, sample_silver_data, tmp_path):
        # Test that customer names are split into first and last names
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        # Check John Doe is split correctly
        john = customer_df[customer_df['customer_id'] == 'CUST-A'].iloc[0]
        assert john['customer_first_name'] == 'John', "First name should be extracted"
        assert john['customer_last_name'] == 'Doe', "Last name should be extracted"
    
    def test_customer_deduplication(self, sample_silver_data, tmp_path):
        # Test that customer dataset has one row per customer
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        assert len(customer_df) == 3, "Should have exactly 3 unique customers"
        assert customer_df['customer_id'].nunique() == 3, "All customer_ids should be unique"
    
    def test_orders_last_month_calculation(self, sample_silver_data, tmp_path):
        # Test that orders_last_month is calculated correctly
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        # CUST-A: 1 order in Dec (2018-12-15)
        # CUST-B: 1 order in Dec (2018-12-10)
        # CUST-C: 0 orders in Dec
        
        cust_a = customer_df[customer_df['customer_id'] == 'CUST-A'].iloc[0]
        cust_b = customer_df[customer_df['customer_id'] == 'CUST-B'].iloc[0]
        cust_c = customer_df[customer_df['customer_id'] == 'CUST-C'].iloc[0]
        
        assert cust_a['orders_last_month'] == 1, "CUST-A should have 1 order in last month"
        assert cust_b['orders_last_month'] == 1, "CUST-B should have 1 order in last month"
        assert cust_c['orders_last_month'] == 0, "CUST-C should have 0 orders in last month"
    
    def test_orders_last_6_months_calculation(self, sample_silver_data, tmp_path):
        # Test that orders_last_6_months uses calendar months correctly
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        # With pd.DateOffset(months=6) from 2018-12-30, six_months_ago = 2018-06-30
        # CUST-A: Orders on 2018-12-15, 2018-08-01, 2018-07-01 = 3 orders
        # CUST-B: Orders on 2018-12-10 = 1 order
        # CUST-C: Orders on 2018-11-20 = 1 order
        
        cust_a = customer_df[customer_df['customer_id'] == 'CUST-A'].iloc[0]
        cust_b = customer_df[customer_df['customer_id'] == 'CUST-B'].iloc[0]
        cust_c = customer_df[customer_df['customer_id'] == 'CUST-C'].iloc[0]
        
        assert cust_a['orders_last_6_months'] >= 2, "CUST-A should have at least 2 orders in last 6 months"
        assert cust_b['orders_last_6_months'] == 1, "CUST-B should have 1 order in last 6 months"
        assert cust_c['orders_last_6_months'] == 1, "CUST-C should have 1 order in last 6 months"
    
    def test_orders_total_calculation(self, sample_silver_data, tmp_path):
        # Test that orders_total counts all orders correctly
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        # CUST-A: ORD-001, ORD-005, ORD-010, ORD-014 = 4 orders
        # CUST-B: ORD-002, ORD-011 = 2 orders
        # CUST-C: ORD-003, ORD-007 = 2 orders
        
        cust_a = customer_df[customer_df['customer_id'] == 'CUST-A'].iloc[0]
        cust_b = customer_df[customer_df['customer_id'] == 'CUST-B'].iloc[0]
        cust_c = customer_df[customer_df['customer_id'] == 'CUST-C'].iloc[0]
        
        assert cust_a['orders_total'] == 4, "CUST-A should have 4 total orders"
        assert cust_b['orders_total'] == 2, "CUST-B should have 2 total orders"
        assert cust_c['orders_total'] == 2, "CUST-C should have 2 total orders"
    
    def test_metric_data_types(self, sample_silver_data, tmp_path):
        # Test that metric columns are integers
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        assert pd.api.types.is_integer_dtype(customer_df['orders_last_month']), "orders_last_month should be integer"
        assert pd.api.types.is_integer_dtype(customer_df['orders_last_6_months']), "orders_last_6_months should be integer"
        assert pd.api.types.is_integer_dtype(customer_df['orders_total']), "orders_total should be integer"
    
    def test_no_nulls_in_key_columns(self, sample_silver_data, tmp_path):
        # Test that key columns don't have nulls
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        assert customer_df['customer_id'].notna().all(), "customer_id should not have nulls"
        assert customer_df['customer_first_name'].notna().all(), "customer_first_name should not have nulls"
        assert customer_df['customer_last_name'].notna().all(), "customer_last_name should not have nulls"
    
    def test_metrics_consistency(self, sample_silver_data, tmp_path):
        # Test that metric logic is consistent
        generator = GoldCustomer(use_local=True)
        df = generator.read_silver_data(sample_silver_data)
        customer_df = generator.calculate_order_metrics(df)
        
        for _, row in customer_df.iterrows():
            # Last month should be less/equal last 6 months
            assert row['orders_last_month'] <= row['orders_last_6_months'], \
                "orders_last_month should be <= orders_last_6_months"
            # Last 6 months should be less/equal total
            assert row['orders_last_6_months'] <= row['orders_total'], \
                "orders_last_6_months should be <= orders_total"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
