import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType


# Initialize glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_DATABASE', 'BRONZE_TABLE', 
                                      'SILVER_S3_PATH', 'GLUE_DATABASE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
BRONZE_DATABASE = args['BRONZE_DATABASE']
BRONZE_TABLE = args['BRONZE_TABLE']
SILVER_S3_PATH = args['SILVER_S3_PATH']
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


def standardize_column_names(df):
    
    for col in df.columns:
        # Convert "Column Name" to "column_name"
        new_col = col.strip().replace(' ', '_').lower()
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    
    return df


def main():
    # ETL logic for the silver layer
    
    log_info("Starting Silver layer ETL", bronze_database=BRONZE_DATABASE, bronze_table=BRONZE_TABLE)
    
    # 1. Read bronze data from glue catalog
    log_info("Reading bronze data from glue catalog")
    bronze_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name=BRONZE_TABLE,
        transformation_ctx="bronze_dyf"
    )
    
    # Convert to Spark df for transformations
    df = bronze_dyf.toDF()
    
    log_info("Bronze data loaded", record_count=df.count(), columns=len(df.columns))
    
    # 2. Remove duplicates based on row ID
    initial_count = df.count()
    df = df.dropDuplicates(['Row ID'])
    duplicates_removed = initial_count - df.count()
    
    if duplicates_removed > 0:
        log_info("Duplicates removed", count=duplicates_removed)
    
    # 3. Parse and validate dates with DD/MM/YYYY format
    df = df.withColumn('Order Date', F.to_date(F.col('Order Date')))
    
    # For ship date, parse with DD/MM/YYYY format
    df = df.withColumn(
        'Ship Date',
        F.when(
            F.to_date(F.col('Ship Date'), 'dd/MM/yyyy').isNotNull(),
            F.to_date(F.col('Ship Date'), 'dd/MM/yyyy')
        ).otherwise(
            F.to_date(F.col('Ship Date'), 'MM/dd/yyyy')  # Fallback
        )
    )
    
    # 4. Check for invalid dates
    invalid_order_dates = df.filter(F.col('Order Date').isNull()).count()
    invalid_ship_dates = df.filter(F.col('Ship Date').isNull()).count()
    
    if invalid_order_dates > 0:
        log_info("Invalid order dates found", count=invalid_order_dates)
    
    if invalid_ship_dates > 0:
        log_info("Invalid ship dates found", count=invalid_ship_dates)
    
    # 5. Remove records with null order date
    df = df.filter(F.col('Order Date').isNotNull())
    null_dates_removed = initial_count - duplicates_removed - df.count()
    
    if null_dates_removed > 0:
        log_info("Records with null Order Date removed", count=null_dates_removed)
    
    # 6. Validate numeric fields
    df = df.withColumn('Sales', F.col('Sales').cast(DoubleType()))
    
    # 7. Trim string fields
    string_columns = [field.name for field in df.schema.fields 
                     if isinstance(field.dataType, StringType)]
    
    for col_name in string_columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    
    # 8. Standardize column names
    df = standardize_column_names(df)
    
    log_info("Data cleansing completed", 
             records_output=df.count(),
             duplicates_removed=duplicates_removed,
             null_dates_removed=null_dates_removed)
    
    # 9. Add partition columns for writing
    df = df.withColumn('year', F.year(F.col('order_date')))
    df = df.withColumn('month', F.month(F.col('order_date')))
    df = df.withColumn('day', F.dayofmonth(F.col('order_date')))
    
    # 10. Write to Silver layer (S3) partitioned by order date
    log_info("Writing silver data to S3", s3_path=SILVER_S3_PATH)
    
    df.write \
        .mode('overwrite') \
        .partitionBy('year', 'month', 'day') \
        .parquet(SILVER_S3_PATH, compression='snappy')
    
    log_info("Silver data written successfully", 
             total_records=df.count(),
             output_path=SILVER_S3_PATH)
    
    # 11. Update glue catalog (crawler will run after this job)
    log_info("Silver layer ETL completed successfully")
    
    # Commit job
    job.commit()


if __name__ == "__main__":
    main()
