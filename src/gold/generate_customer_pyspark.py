import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DateType


# Initialize glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_DATABASE', 'SILVER_TABLE',
                                      'GOLD_CUSTOMER_S3_PATH', 'GLUE_DATABASE',
                                      'DATASET_LATEST_DATE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SILVER_DATABASE = args['SILVER_DATABASE']
SILVER_TABLE = args['SILVER_TABLE']
GOLD_CUSTOMER_S3_PATH = args['GOLD_CUSTOMER_S3_PATH']
GLUE_DATABASE = args['GLUE_DATABASE']
DATASET_LATEST_DATE = args.get('DATASET_LATEST_DATE', '2018-12-30')  # Default from assignment


def log_info(message, **kwargs):
    # Structured logging for cloudWatch
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "message": message,
        "job_name": args['JOB_NAME']
    }
    log_data.update(kwargs)
    print(f"LOG: {log_data}")


def main():
    # ETL logic for gold customer layer
    
    log_info("Starting gold customer layer ETL")
    
    # 1. Read silver data from glue catalog
    log_info("Reading silver data from glue catalog", database=SILVER_DATABASE, table=SILVER_TABLE)
    
    silver_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name=SILVER_TABLE,
        transformation_ctx="silver_dyf"
    )
    
    # Convert to spark df
    df = silver_dyf.toDF()
    
    log_info("Silver data loaded", record_count=df.count())
    
    # 2. Parse customer names into first and last name
    df = df.withColumn(
        'customer_first_name',
        F.split(F.col('customer_name'), ' ').getItem(0)
    )
    
    # For last name, take everything after the first space
    df = df.withColumn(
        'name_parts',
        F.split(F.col('customer_name'), ' ')
    )
    
    df = df.withColumn(
        'customer_last_name',
        F.when(
            F.size(F.col('name_parts')) > 1,
            F.element_at(F.col('name_parts'), -1)  # Get last element
        ).otherwise(F.col('customer_first_name'))  # If single name, use it as last name too
    ).drop('name_parts')
    
    # 3. Define date thresholds
    latest_date = F.lit(DATASET_LATEST_DATE).cast(DateType())
    
    # For one month ago, subtract 1 month from latest date
    one_month_ago = F.add_months(latest_date, -1)
    # For six months ago, subtract 6 months from latest date
    six_months_ago = F.add_months(latest_date, -6)
    
    log_info("Date thresholds calculated",
             latest_date=DATASET_LATEST_DATE,
             one_month_ago=str(one_month_ago),
             six_months_ago=str(six_months_ago))
    
    # 4. Calculate order metrics per customer
    orders_df = df.select(
        'customer_id',
        'customer_first_name',
        'customer_last_name',
        'segment',
        'country',
        'order_id',
        'order_date'
    ).dropDuplicates(['customer_id', 'order_id'])
    
    # 5. Calculate orders_last_month
    orders_last_month = orders_df \
        .filter(F.col('order_date') >= one_month_ago) \
        .groupBy('customer_id') \
        .agg(F.countDistinct('order_id').alias('orders_last_month'))
    
    # 6. Calculate orders_last_6_months
    orders_last_6_months = orders_df \
        .filter(F.col('order_date') >= six_months_ago) \
        .groupBy('customer_id') \
        .agg(F.countDistinct('order_id').alias('orders_last_6_months'))
    
    # 7. Calculate orders_total
    orders_total = orders_df \
        .groupBy('customer_id') \
        .agg(F.countDistinct('order_id').alias('orders_total'))
    
    # 8. Get customer attributes (one row per customer)
    customer_attrs = orders_df \
        .groupBy('customer_id') \
        .agg(
            F.first('customer_first_name').alias('customer_first_name'),
            F.first('customer_last_name').alias('customer_last_name'),
            F.first('segment').alias('customer_segment'),
            F.first('country').alias('country'),
            F.min('order_date').alias('first_order_date')  # For partitioning
        )
    
    # 9. Merge all metrics by using left join to ensure all customers appear
    customer_df = customer_attrs \
        .join(orders_total, on='customer_id', how='left') \
        .join(orders_last_6_months, on='customer_id', how='left') \
        .join(orders_last_month, on='customer_id', how='left')
    
    # 10. Fill the null values with 0 
    customer_df = customer_df \
        .fillna(0, subset=['orders_last_month', 'orders_last_6_months', 'orders_total'])
    
    # 11. Ensure for correct data types
    customer_df = customer_df.select(
        F.col('customer_id').cast(StringType()),
        F.col('customer_first_name').cast(StringType()),
        F.col('customer_last_name').cast(StringType()),
        F.col('customer_segment').cast(StringType()),
        F.col('country').cast(StringType()),
        F.col('orders_last_month').cast(IntegerType()),
        F.col('orders_last_6_months').cast(IntegerType()),
        F.col('orders_total').cast(IntegerType()),
        F.col('first_order_date').cast(DateType())
    )
    
    log_info("Customer metrics calculated",
             unique_customers=customer_df.count(),
             total_orders_sum=customer_df.agg(F.sum('orders_total')).collect()[0][0])
    
    # 12. Add partition columns based on first order date
    customer_df = customer_df.withColumn('year', F.year(F.col('first_order_date')))
    customer_df = customer_df.withColumn('month', F.month(F.col('first_order_date')))
    customer_df = customer_df.withColumn('day', F.dayofmonth(F.col('first_order_date')))
    
    # Drop first_order_date, it is only used for partitioning
    customer_df = customer_df.drop('first_order_date')
    
    # 13. Validate metrics consistency
    invalid_metrics = customer_df.filter(
        (F.col('orders_last_month') > F.col('orders_last_6_months')) |
        (F.col('orders_last_6_months') > F.col('orders_total'))
    ).count()
    
    if invalid_metrics > 0:
        log_info("WARNING: inconsistent metrics found", count=invalid_metrics)
    
    # 14. Write to gold customer layer (S3) partitioned by first order date
    log_info("Writing gold customer data to S3", s3_path=GOLD_CUSTOMER_S3_PATH)
    
    final_count = customer_df.count()
    
    customer_df.write \
        .mode('overwrite') \
        .partitionBy('year', 'month', 'day') \
        .parquet(GOLD_CUSTOMER_S3_PATH, compression='snappy')
    
    log_info("Gold customer data written successfully",
             total_records=final_count,
             output_path=GOLD_CUSTOMER_S3_PATH)
    
    # 15. Commit job
    log_info("Gold customer layer ETL completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
