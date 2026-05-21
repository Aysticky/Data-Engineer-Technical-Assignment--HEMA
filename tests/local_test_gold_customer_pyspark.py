import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def create_local_spark_session():
    # Create local spark session for testing
    return SparkSession.builder \
        .appName("HEMA-Gold-Customer-Local-Test") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


def test_gold_customer_pyspark():
    # Test gold customer layer pyspark transformations locally
    
    spark = create_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    silver_pyspark_path = str(project_root / "data" / "silver-pyspark-test")
    silver_pandas_path = str(project_root / "data" / "silver")
    output_path = str(project_root / "data" / "gold" / "customer-pyspark-test")
    
    try:
        # Try reading pyspark silver output first
        if Path(silver_pyspark_path).exists():
            silver_path = silver_pyspark_path
            print(f"\nInput: {silver_path} (pySpark silver)")
        else:
            silver_path = silver_pandas_path
            print(f"\nInput: {silver_path} (pandas silver to fallback)")
        
        print(f"Output: {output_path}")
        
        # Read silver data
        print("\nReading silver parquet data")
        silver_df = spark.read.parquet(silver_path)
        input_count = silver_df.count()
        print(f"Loaded {input_count:,} records")
        
        # Show sample before transformation
        print("\nSample silver data for first 3 rows:")
        silver_df.select('customer_id', 'customer_name', 'segment', 'order_date').show(3, truncate=False)
        
        # Gold customer transformations
               
        # 1. Determine reference date (latest date in dataset)
        latest_date = silver_df.agg(F.max('order_date')).collect()[0][0]
        print(f"Latest date in dataset: {latest_date}")
        
        # 2. Parse customer names
        df = silver_df.withColumn(
            'customer_first_name',
            F.split(F.col('customer_name'), ' ').getItem(0)
        ).withColumn(
            'customer_last_name',
            F.element_at(F.split(F.col('customer_name'), ' '), -1)
        )
        print("split customer_name into first_name and last_name")
        
        # Show name parsing sample
        df.select('customer_name', 'customer_first_name', 'customer_last_name').distinct().show(5, truncate=False)
        
        # 3. Calculate date thresholds using calendar months
        six_months_ago = F.add_months(F.lit(latest_date), -6)
        one_month_ago = F.add_months(F.lit(latest_date), -1)
        
        six_months_ago_value = spark.sql(f"SELECT add_months(date'{latest_date}', -6)").collect()[0][0]
        one_month_ago_value = spark.sql(f"SELECT add_months(date'{latest_date}', -1)").collect()[0][0]
        
        print(f"Reference date:{latest_date}")
        print(f"Six months ago:{six_months_ago_value}")
        print(f"One month ago:{one_month_ago_value}")
        
        # 4. Calculate metrics for each time period
        
        # Total orders
        print("Calculating total orders")
        total_orders = df.groupBy('customer_id').agg(
            F.countDistinct('order_id').alias('orders_total')
        )
        
        # Last 6 months
        print("Calculating last 6 months orders")
        last_6_months = df.filter(
            F.col('order_date') > six_months_ago
        ).groupBy('customer_id').agg(
            F.countDistinct('order_id').alias('orders_last_6_months')
        )
        
        # Last month
        print("Calculating last month orders")
        last_month = df.filter(
            F.col('order_date') > one_month_ago
        ).groupBy('customer_id').agg(
            F.countDistinct('order_id').alias('orders_last_month')
        )
        
        # 5. Get base customer info
        print("Getting base customer information")
        customer_base = df.select(
            'customer_id',
            'customer_first_name',
            'customer_last_name',
            'segment',
            'country'
        ).distinct()
        
        unique_customers = customer_base.count()
        print(f"{unique_customers:,} unique customers")
        
        # 6. Join all metrics
        print("Joining all metrics")
        customer_df = customer_base \
            .join(total_orders, on='customer_id', how='left') \
            .join(last_6_months, on='customer_id', how='left') \
            .join(last_month, on='customer_id', how='left')
        
        # Fill nulls with 0 for time-period metrics
        customer_df = customer_df.fillna(0, subset=['orders_last_month', 'orders_last_6_months'])
        
        # Rename segment to customer_segment
        customer_df = customer_df.withColumnRenamed('segment', 'customer_segment')
        
        # Validation
        print("\nTransformation completed!")
        print(f"\nSummary:")
        print(f"Input records:{input_count:,}")
        print(f"Unique customers:{unique_customers:,}")
        
        # Verify schema matches assignment requirements
        expected_columns = [
            'customer_id', 'customer_first_name', 'customer_last_name',
            'customer_segment', 'country',
            'orders_last_month', 'orders_last_6_months', 'orders_total'
        ]
        actual_columns = customer_df.columns
        
        print("\nOutput schema validation:")
        print(f"Expected columns: {expected_columns}")
        print(f"Actual columns:   {actual_columns}")
        
        schema_match = set(actual_columns) == set(expected_columns)
        if schema_match:
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
        customer_df.printSchema()
        
        # Show sample output
        print("\nSample gold customer data for first 10 rows:")
        customer_df.orderBy(F.desc('orders_total')).show(10, truncate=False)
        
        # Data quality checks
        
        # 1. Check for nulls
        print("\nNull check:")
        for col in customer_df.columns:
            null_count = customer_df.filter(F.col(col).isNull()).count()
            status = "Yes" if null_count == 0 else "No"
            print(f"{status} {col}: {null_count} nulls")
        
        # 2. Validate metric consistency
        print("\nMetric consistency check:")
        inconsistent = customer_df.filter(
            (F.col('orders_last_month') > F.col('orders_last_6_months')) |
            (F.col('orders_last_6_months') > F.col('orders_total'))
        )
        inconsistent_count = inconsistent.count()
        
        if inconsistent_count == 0:
            print("All metrics are consistent (last_month ≤ last_6_months ≤ total)")
        else:
            print(f"{inconsistent_count} customers have inconsistent metrics")
            print("Sample inconsistent records:")
            inconsistent.select('customer_id', 'orders_last_month', 
                              'orders_last_6_months', 'orders_total').show(5)
        
        # 3. Statistics
        print("\nOrder statistics:")
        stats = customer_df.select(
            F.avg('orders_total').alias('avg_total'),
            F.max('orders_total').alias('max_total'),
            F.avg('orders_last_6_months').alias('avg_6m'),
            F.avg('orders_last_month').alias('avg_1m')
        ).collect()[0]
        
        print(f"Average total orders: {stats['avg_total']:.2f}")
        print(f"Max total orders: {stats['max_total']}")
        print(f"Average 6-month orders: {stats['avg_6m']:.2f}")
        print(f"Average 1-month orders: {stats['avg_1m']:.2f}")
        
        # 4. Segment distribution
        print("\nCustomer segment distribution:")
        customer_df.groupBy('customer_segment').agg(
            F.count('*').alias('customers'),
            F.avg('orders_total').alias('avg_orders')
        ).orderBy(F.desc('customers')).show()
        
        # 5. Top customers
        customer_df.select(
            'customer_id', 'customer_first_name', 'customer_last_name',
            'orders_last_month', 'orders_last_6_months', 'orders_total'
        ).orderBy(F.desc('orders_total')).show(10, truncate=False)
        
        # 6. Recent activity analysis
        active_last_month = customer_df.filter(F.col('orders_last_month') > 0).count()
        active_last_6_months = customer_df.filter(F.col('orders_last_6_months') > 0).count()
        
        print(f"Customers with orders in last month: {active_last_month:,} ({active_last_month/unique_customers*100:.1f}%)")
        print(f"Customers with orders in last 6 months: {active_last_6_months:,} ({active_last_6_months/unique_customers*100:.1f}%)")
        
        # Writing the output
        print(f"\nWriting output to: {output_path}")
        customer_df.write.mode('overwrite').parquet(output_path)
        
        # Verify written data
        verification_df = spark.read.parquet(output_path)
        written_count = verification_df.count()
        print(f"Verified {written_count:,} records in output")
        
        if written_count == unique_customers:
            print("Record count matches")
        else:
            print(f"Record count mismatch. Expected {unique_customers:,}, got {written_count:,}")
        
        # Verify schema after write/read
        if set(verification_df.columns) == set(expected_columns):
            print("Schema preserved after write/read")
        
        return True
        
    except Exception as e:
        print(f"\nError during gold customer test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


if __name__ == '__main__':
    success = test_gold_customer_pyspark()
    sys.exit(0 if success else 1)
