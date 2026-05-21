import pytest
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from bronze.ingest_raw_data import BronzeIngestion
from utils.config import Config


@pytest.fixture
def sample_data():
    # using sample dataset
    return pd.DataFrame({
        'Row ID': [1, 2, 3],
        'Order ID': ['CA-2018-001', 'CA-2018-002', 'CA-2018-003'],
        'Order Date': ['12/01/2018', '12/15/2018', '12/30/2018'],
        'Ship Date': ['12/05/2018', '12/20/2018', '12/31/2018'],
        'Ship Mode': ['First Class', 'Standard Class', 'Same Day'],
        'Customer ID': ['JD-001', 'JS-002', 'MW-003'],
        'Customer Name': ['John Doe', 'Jane Smith', 'Mike Wilson'],
        'Segment': ['Consumer', 'Corporate', 'Home Office'],
        'Country': ['United States', 'United States', 'United States'],
        'City': ['New York', 'Boston', 'Chicago']
    })


@pytest.fixture
def sample_csv_file(tmp_path, sample_data):
    csv_file = tmp_path / "test_data.csv"
    sample_data.to_csv(csv_file, index=False)
    return str(csv_file)


def test_bronze_ingestion_initialization():
    ingestion = BronzeIngestion(use_local=True)
    assert ingestion.use_local == True
    assert ingestion.partition_manager is not None


def test_read_source_data(sample_csv_file):
    ingestion = BronzeIngestion(use_local=True)
    df = ingestion.read_source_data(sample_csv_file)
    
    assert len(df) == 3
    assert 'Order ID' in df.columns
    assert 'Customer Name' in df.columns


def test_write_bronze_data_creates_partitions(tmp_path, sample_data):
    ingestion = BronzeIngestion(use_local=True)
    output_path = str(tmp_path / "bronze")
    
    # Convert dates
    sample_data['Order Date'] = pd.to_datetime(sample_data['Order Date'], format='%m/%d/%Y')
    
    ingestion.write_bronze_data(sample_data, output_path)
    
    # Check that partition directories were created
    assert os.path.exists(output_path)
    
    # Check for year partition
    year_dirs = [d for d in os.listdir(output_path) if d.startswith('year=')]
    assert len(year_dirs) > 0


def test_bronze_run_end_to_end(sample_csv_file, tmp_path):
    original_output = Config.get_local_output_path
    Config.get_local_output_path = lambda layer: str(tmp_path / layer)
    
    ingestion = BronzeIngestion(use_local=True)
    metrics = ingestion.run(sample_csv_file)
    
    assert metrics['status'] == 'success'
    assert metrics['records_processed'] == 3
    assert 'execution_time_seconds' in metrics
    
    # Restore original
    Config.get_local_output_path = original_output
