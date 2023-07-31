import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, TimestampType
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

spark = SparkSession.builder \
                    .master(os.getenv('SPARK_MASTER_HOST', 'local')) \
                    .appName('taxi-etl') \
                    .config("spark.executor.memory", "8g") \
                    .config("spark.driver.memory", "8g") \
                    .getOrCreate()

START_YEAR = 2023
START_MONTH = 1
END_YEAR = 2023
END_MONTH = 5

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    
    # Create custom schema for the files
    print("Extract process started.")
    custom_schema = StructType([StructField('VendorID', LongType(), True),
                                StructField('tpep_pickup_datetime', TimestampType(), True),
                                StructField('tpep_dropoff_datetime', TimestampType(), True),
                                StructField('passenger_count', DoubleType(), True),
                                StructField('trip_distance', DoubleType(), True),
                                StructField('RatecodeID', LongType(), True),
                                StructField('store_and_fwd_flag', StringType(), True),
                                StructField('PULocationID', LongType(), True),
                                StructField('DOLocationID', LongType(), True),
                                StructField('payment_type', LongType(), True),
                                StructField('fare_amount', DoubleType(), True),
                                StructField('extra', DoubleType(), True),
                                StructField('mta_tax', DoubleType(), True),
                                StructField('tip_amount', DoubleType(), True),
                                StructField('tolls_amount', DoubleType(), True),
                                StructField('improvement_surcharge', DoubleType(), True),
                                StructField('total_amount', DoubleType(), True),
                                StructField('congestion_surcharge', DoubleType(), True),
                                StructField('airport_fee', DoubleType(), True)
    ])

        # Generate a list of months between the start and end years and months
    def generate_month_list(start_year, start_month, end_year, end_month):
        current_year = start_year
        current_month = start_month
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            yield f"{current_year:04d}-{current_month:02d}"
            current_month += 1
            if current_month > 12:
                current_year += 1
                current_month = 1

    # Create a list of partition paths
    root = 'https://storage.googleapis.com/taxi-data-elt-bucket/raw_data/report_date%3D'
    partition_paths = [f"{root}{partition}/trips_{partition}.parquet" for partition in generate_month_list(START_YEAR, START_MONTH, END_YEAR, END_MONTH)]

    # Read the partitioned files
    print("Reading files from API...")
    for i in range(len(partition_paths)):
        url = partition_paths[i]
        response = requests.get(url)
        if i == 0:
            df = spark.read.schema(custom_schema).parquet(io.StringIO(response.text))
        else:
            new_df = spark.read.schema(custom_schema).parquet(io.StringIO(response.text))
            df = df.union(new_df)
        print(f"Read {i} parquet files into Spark DataFrame...")
    
    # Clean the column names and perform type casting, add primary key trip_id
    df = df.withColumnRenamed("VendorID", "vendor_id") \
           .withColumnRenamed("PULocationID", "pick_up_location_id") \
           .withColumnRenamed("DOLocationID", "drop_off_location_id") \
           .withColumnRenamed("RatecodeID", "rate_code_id") \
           .withColumnRenamed("payment_type", "payment_type_id") \
           .withColumn("passenger_count", col("passenger_count").cast(LongType())) \
           .withColumn("trip_id", monotonically_increasing_id())
        
    print("Extract process completed.")

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'