import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Initialize glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_DATABASE', 'SILVER_TABLE',
                                      'GOLD_SALES_S3_PATH', 'GLUE_DATABASE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SILVER_DATABASE = args['SILVER_DATABASE']
SILVER_TABLE = args['SILVER_TABLE']
GOLD_SALES_S3_PATH = args['GOLD_SALES_S3_PATH']
GLUE_DATABASE = args['GLUE_DATABASE']


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
    # ETL logic for the gold sales layer
    
    log_info("Starting Gold Sales layer ETL")
    
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
    
    # 2. Select required columns for sales dataset
    # Required columns are order_id, order_date, shipment_date, shipment_mode, city
    sales_df = df.select(
        F.col('order_id'),
        F.col('order_date'),
        F.col('ship_date').alias('shipment_date'),  # Rename ship_date to shipment_date
        F.col('ship_mode').alias('shipment_mode'),  # Rename ship_mode to shipment_mode
        F.col('city')
    )
    
    # 3. Deduplicate by order_id and keep first occurrence and use window function to assign row number per order_id
    window_spec = Window.partitionBy('order_id').orderBy('order_date')
    sales_df = sales_df.withColumn('row_num', F.row_number().over(window_spec))
    sales_df = sales_df.filter(F.col('row_num') == 1).drop('row_num')
    
    log_info("Sales dataset generated", 
             unique_orders=sales_df.count(),
             columns=len(sales_df.columns))
    
    # 4. Ensure all columns have correct data types
    sales_df = sales_df.select(
        F.col('order_id').cast('string'),
        F.col('order_date').cast('date'),
        F.col('shipment_date').cast('date'),
        F.col('shipment_mode').cast('string'),
        F.col('city').cast('string')
    )
    
    # 5. Validate no nulls in critical columns
    null_order_ids = sales_df.filter(F.col('order_id').isNull()).count()
    null_order_dates = sales_df.filter(F.col('order_date').isNull()).count()
    
    if null_order_ids > 0:
        log_info("WARNING: Null order_ids found", count=null_order_ids)
    
    if null_order_dates > 0:
        log_info("WARNING: Null order_dates found", count=null_order_dates)
    
    # 6. Add partition columns for writing
    sales_df = sales_df.withColumn('year', F.year(F.col('order_date')))
    sales_df = sales_df.withColumn('month', F.month(F.col('order_date')))
    sales_df = sales_df.withColumn('day', F.dayofmonth(F.col('order_date')))
    
    # 7. Write to gold sales layer (S3) partitioned by order_date
    log_info("Writing gold sales data to S3", s3_path=GOLD_SALES_S3_PATH)
    
    # Get count before writing
    final_count = sales_df.count()
    
    sales_df.write \
        .mode('overwrite') \
        .partitionBy('year', 'month', 'day') \
        .parquet(GOLD_SALES_S3_PATH, compression='snappy')
    
    log_info("Gold sales data written successfully",
             total_records=final_count,
             output_path=GOLD_SALES_S3_PATH)
    
    # 8. Commit job
    log_info("Gold sales layer ETL completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
