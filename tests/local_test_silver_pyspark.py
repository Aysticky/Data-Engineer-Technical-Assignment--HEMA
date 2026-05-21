import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def create_local_spark_session():
    # Create local Spark session for testing
    return SparkSession.builder \
        .appName("HEMA-Silver-Local-Test") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


def test_silver_layer_pyspark():
    # Test silver layer PySpark transformations locally
    
    print("Local silver layer pyspark test")
    
    # Initialize Spark
    spark = create_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define paths from local filesystem
    bronze_path = str(project_root / "data" / "bronze")
    output_path = str(project_root / "data" / "silver-pyspark-test")
    
    print(f"\nInput: {bronze_path}")
    print(f"Output: {output_path}")
    
    try:
        # Read bronze data
        print("\nreading bronze parquet data")
        bronze_df = spark.read.parquet(bronze_path)
        input_count = bronze_df.count()
        print(f"Loaded {input_count:,} records")
        
        # Show sample before transformation
        print("\nsample Bronze data for first 3 rows:")
        bronze_df.select('Row ID', 'Order Date', 'Ship Date', 'Customer Name').show(3, truncate=False)
        
        # Silver layer transformations    
        print("\nApplying transformations")
        
        # 1. Remove duplicates by row ID
        print("Removing duplicates by row ID")
        df = bronze_df.dropDuplicates(['Row ID'])
        after_dedup = df.count()
        print(f"{input_count - after_dedup} duplicates removed ({after_dedup:,} records remain)")
        
        # 2. Parse dates with DD/MM/YYYY format
        print("Parsing dates")
        df = df.withColumn(
            'Order Date',
            F.to_date(F.col('Order Date'), 'dd/MM/yyyy')
        )
        df = df.withColumn(
            'Ship Date',
            F.to_date(F.col('Ship Date'), 'dd/MM/yyyy')
        )
        
        # Validate date parsing
        null_order_dates = df.filter(F.col('Order Date').isNull()).count()
        null_ship_dates = df.filter(F.col('Ship Date').isNull()).count()
        print(f"Order date nulls: {null_order_dates}")
        print(f"Ship date nulls: {null_ship_dates}")
        
        if null_order_dates > 0 or null_ship_dates > 0:
            print("Some dates failed to parse")
        
        # 3. Filter out invalid records
        print("Filtering invalid records")
        df = df.filter(
            (F.col('Order Date').isNotNull()) &
            (F.col('Ship Date').isNotNull()) &
            (F.col('Sales').isNotNull()) &
            (F.col('Quantity').isNotNull())
        )
        after_filter = df.count()
        print(f"{after_dedup - after_filter} invalid records removed ({after_filter:,} records remain)")
        
        # 4. Standardize column names to snake_case
        print("Standardizing column names")
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
            'City': 'city',
            'State': 'state',
            'Postal Code': 'postal_code',
            'Region': 'region',
            'Product ID': 'product_id',
            'Category': 'category',
            'Sub-Category': 'sub_category',
            'Product Name': 'product_name',
            'Sales': 'sales',
            'Quantity': 'quantity',
            'Discount': 'discount',
            'Profit': 'profit'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        print(f"Renamed {len(column_mapping)} columns")
        
        # 5. Add year/month/day partition columns
        print("Adding partition columns (year/month/day)")
        df = df.withColumn('year', F.year(F.col('order_date')))
        df = df.withColumn('month', F.month(F.col('order_date')))
        df = df.withColumn('day', F.dayofmonth(F.col('order_date')))
        print("Partition columns added")
        
        # Validation      
        print("\nTransformation completed!")
        print(f"Input records:{input_count:,}")
        print(f"Output records:{after_filter:,}")
        print(f"Success rate:{after_filter/input_count*100:.1f}%")
        
        # Show schema
        print("\nOutput Schema:")
        df.printSchema()
        
        # Show sample output
        print("\nSample silver data for first 5 rows:")
        df.select('row_id', 'order_id', 'order_date', 'ship_date', 'customer_name', 'sales').show(5, truncate=False)
        
        # Data quality checks
        print("\nData quality checks:")
        print(f"Unique row_ids: {df.select('row_id').distinct().count():,}")
        print(f"Unique orders: {df.select('order_id').distinct().count():,}")
        print(f"Date range: {df.agg(F.min('order_date')).collect()[0][0]} to {df.agg(F.max('order_date')).collect()[0][0]}")
        
        # Check for nulls in critical columns
        print("\nNull Check (critical columns):")
        for col in ['order_id', 'order_date', 'ship_date', 'customer_id', 'sales']:
            null_count = df.filter(F.col(col).isNull()).count()
            print(f"   {col}: {null_count} nulls")
        
        # Writing the output       
        print(f"\nWriting output to: {output_path}")
        df.write.mode('overwrite').partitionBy('year', 'month', 'day').parquet(output_path)
        print("Parquet files written successfully")
        
        # Verify written data
        print("\nVerifying written data")
        verification_df = spark.read.parquet(output_path)
        written_count = verification_df.count()
        print(f"Verified {written_count:,} records in output")
        
        if written_count == after_filter:
            print("Record count matches!")
        else:
            print(f"Record count mismatch! Expected {after_filter:,}, got {written_count:,}")
              
        return True
        
    except Exception as e:
        print(f"\nERROR during Silver layer test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


if __name__ == '__main__':
    success = test_silver_layer_pyspark()
    sys.exit(0 if success else 1)
