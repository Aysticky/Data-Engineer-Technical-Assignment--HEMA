from datetime import datetime
from typing import Dict, List
import pandas as pd


class PartitionManager:
   
    @staticmethod
    def get_partition_path(date: datetime, base_path: str, table_name: str = None) -> str:
        
        year = date.year
        month = str(date.month).zfill(2)
        day = str(date.day).zfill(2)
        
        if table_name:
            return f"{base_path.rstrip('/')}/{table_name}/year={year}/month={month}/day={day}/"
        else:
            return f"{base_path.rstrip('/')}/year={year}/month={month}/day={day}/"
    
    @staticmethod
    def get_partition_columns() -> List[str]:
       
        return ['year', 'month', 'day']
    
    @staticmethod
    def add_partition_columns(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
       
        df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])
        
        df['year'] = df[date_column].dt.year
        df['month'] = df[date_column].dt.month.astype(str).str.zfill(2)
        df['day'] = df[date_column].dt.day.astype(str).str.zfill(2)
        
        return df
    
    @staticmethod
    def get_partition_filter(
        year: int = None, 
        month: int = None, 
        day: int = None
    ) -> Dict[str, str]:
        
        filters = {}
        
        if year is not None:
            filters['year'] = str(year)
        if month is not None:
            filters['month'] = str(month).zfill(2)
        if day is not None:
            filters['day'] = str(day).zfill(2)
        
        return filters
    
    @staticmethod
    def generate_partition_predicate(
        year: int = None,
        month: int = None, 
        day: int = None
    ) -> str:
       
        predicates = []
        
        if year is not None:
            predicates.append(f"year={year}")
        if month is not None:
            predicates.append(f"month={str(month).zfill(2)}")
        if day is not None:
            predicates.append(f"day={str(day).zfill(2)}")
        
        return " AND ".join(predicates) if predicates else ""
    
    @staticmethod
    def list_partitions_in_date_range(
        start_date: datetime,
        end_date: datetime,
        base_path: str,
        table_name: str = None
    ) -> List[str]:
        
        partition_paths = []
        current_date = start_date
        
        while current_date <= end_date:
            path = PartitionManager.get_partition_path(
                current_date, 
                base_path, 
                table_name
            )
            partition_paths.append(path)
            current_date = current_date + pd.Timedelta(days=1)
        
        return partition_paths
