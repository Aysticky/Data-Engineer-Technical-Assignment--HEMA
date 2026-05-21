import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def create_local_spark_session():
    # Create local spark session for testing
    return SparkSession.builder \
        .appName("HEMA-Gold-Sales-Local-Test") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


def test_gold_sales_pyspark():
    # Test gold sales layer pyspark transformations locally
        
    spark = create_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Try pyspark Silver first, fall back to pandas silver
    silver_pyspark_path = str(project_root / "data" / "silver-pyspark-test")
    silver_pandas_path = str(project_root / "data" / "silver")
    output_path = str(project_root / "data" / "gold" / "sales-pyspark-test")
    
    try:
        if Path(silver_pyspark_path).exists():
            silver_path = silver_pyspark_path
            print(f"\nInput: {silver_path} (PySpark silver)")
        else:
            silver_path = silver_pandas_path
            print(f"\nInput: {silver_path} (Pandas silver to fallback)")
        
        print(f"Output: {output_path}")
        
        # Read silver data
        silver_df = spark.read.parquet(silver_path)
        input_count = silver_df.count()
        print(f"Loaded {input_count:,} records")
        
        # Show sample before transformation
        print("\nSample silver data for first 3 rows:")
        silver_df.select('order_id', 'order_date', 'ship_date', 'ship_mode', 'city').show(3, truncate=False)
        
        # Gold sales transformation
            
        # 1. Select required columns
        print("Selecting required columns")
        sales_df = silver_df.select(
            'order_id',
            'order_date',
            'ship_date',
            'ship_mode',
            'city'
        )
        
        # 2. Deduplicate by order_id amd keep first occurrence
        print("Deduplicating by order_id using window functions")
        window_spec = Window.partitionBy('order_id').orderBy('order_date')
        sales_df = sales_df.withColumn('row_num', F.row_number().over(window_spec))
        sales_df = sales_df.filter(F.col('row_num') == 1).drop('row_num')
        
        after_dedup = sales_df.count()
        print(f"{input_count - after_dedup} duplicates removed ({after_dedup:,} unique orders remain)")
        
        # 3. Rename columns as per assignment requirements
        print("Renaming columns")
        sales_df = sales_df \
            .withColumnRenamed('ship_date', 'shipment_date') \
            .withColumnRenamed('ship_mode', 'shipment_mode')
        
        
        # Validation
        
        print("\nTransformation completed!")
        print(f"\nSummary:")
        print(f"Input records:{input_count:,}")
        print(f"Output records:{after_dedup:,}")
        print(f"Deduplication:{input_count - after_dedup:,} duplicates removed")
        
        # Verify schema matches assignment requirements
        expected_columns = ['order_id', 'order_date', 'shipment_date', 'shipment_mode', 'city']
        actual_columns = sales_df.columns
        
        print("\nOutput schema validation:")
        print(f"Expected columns:{expected_columns}")
        print(f"Actual columns:{actual_columns}")
        
        if actual_columns == expected_columns:
            print("Schema matches assignment requirements")
        else:
            print("Schema mismatch")
            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)
            if missing:
                print(f"Missing: {missing}")
            if extra:
                print(f"Extra: {extra}")
        
        # Show full schema
        sales_df.printSchema()
        
        # Show sample output
        print("\nSample gold sales data for first 10 rows:")
        sales_df.show(10, truncate=False)
        
        # Data quality checks
        print("\nData quality checks:")
        print(f"Unique order_ids: {sales_df.select('order_id').distinct().count():,}")
        print(f"Date range: {sales_df.agg(F.min('order_date')).collect()[0][0]} to {sales_df.agg(F.max('order_date')).collect()[0][0]}")
        print(f"Unique cities: {sales_df.select('city').distinct().count():,}")
        print(f"Unique shipment modes: {sales_df.select('shipment_mode').distinct().count():,}")
        
        # Show shipment mode distribution
        print("\nShipment mode distribution:")
        sales_df.groupBy('shipment_mode').count().orderBy(F.desc('count')).show()
        
        # Check for nulls
        print("\nNull check:")
        for col in sales_df.columns:
            null_count = sales_df.filter(F.col(col).isNull()).count()
            status = "Yes" if null_count == 0 else "No"
            print(f"{status} {col}: {null_count} nulls")
        
        # Writing the output
        print(f"\nWriting output to:{output_path}")
        sales_df.write.mode('overwrite').partitionBy('order_date').parquet(output_path)
        print("Parquet files written successfully (partitioned by order_date)")
        
        # Verify written data
        verification_df = spark.read.parquet(output_path)
        written_count = verification_df.count()
        print(f"Verified {written_count:,} records in output")
        
        if written_count == after_dedup:
            print("Record count matches")
        else:
            print(f"Record count mismatch. Expected {after_dedup:,}, got {written_count:,}")
        
        # Verify schema after write/read
        if verification_df.columns == expected_columns:
            print("Schema preserved after write/read!")
       
        return True
        
    except Exception as e:
        print(f"\nError during gold sales test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


if __name__ == '__main__':
    success = test_gold_sales_pyspark()
    sys.exit(0 if success else 1)
