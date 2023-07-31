from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour
from pyspark.sql.functions import create_map, lit
from pyspark.sql.functions import monotonically_increasing_id, col
from itertools import chain
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # Construct datetime dimension table
    dim_datetime = df.select("tpep_pickup_datetime", "tpep_dropoff_datetime") \
                    .withColumn("pick_up_year", year("tpep_pickup_datetime")) \
                    .withColumn("pick_up_month", month("tpep_pickup_datetime")) \
                    .withColumn("pick_up_day", dayofmonth("tpep_pickup_datetime")) \
                    .withColumn("pick_up_dayofweek", dayofweek("tpep_pickup_datetime")) \
                    .withColumn("pick_up_hour", hour("tpep_pickup_datetime")) \
                    .withColumn("drop_off_year", year("tpep_dropoff_datetime")) \
                    .withColumn("drop_off_month", month("tpep_dropoff_datetime")) \
                    .withColumn("drop_off_day", dayofmonth("tpep_dropoff_datetime")) \
                    .withColumn("drop_off_dayofweek", dayofweek("tpep_dropoff_datetime")) \
                    .withColumn("drop_off_hour", hour("tpep_dropoff_datetime")) \
                    .withColumn("datetime_id", monotonically_increasing_id())

    # Construct rate code dimension table according to mapping at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    rate_code_mapping = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride",
        99:"Unknown"
    }

    mapping_func1 = create_map([lit(x) for x in chain(*rate_code_mapping.items())])
    dim_rate_code = df.select("rate_code_id") \
                    .withColumn("rate_code_type", mapping_func1[col("rate_code_id")]) \
                    .groupBy("rate_code_id", "rate_code_type") \
                    .count() \
                    .orderBy(col("rate_code_id").asc()) \
                    .drop("count")

    # Construct vendor dimension table according to mapping at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    vendor_mapping = {
        1:"Creative Mobile Technologies, LLC;",
        2:"VeriFone Inc.",
        6:"Unknown"
    }

    mapping_func2 = create_map([lit(x) for x in chain(*vendor_mapping.items())])
    dim_vendor = df.select("vendor_id") \
                .withColumn("vendor_info", mapping_func2[col("vendor_id")]) \
                .groupBy("vendor_id", "vendor_info") \
                .count() \
                .orderBy(col("vendor_id").asc()) \
                .drop("count")

    # Construct payment type dimension table according to mapping at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    payment_mapping = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip",
    }

    mapping_func3 = create_map([lit(x) for x in chain(*payment_mapping.items())])
    dim_payment_type = df.select("payment_type_id") \
                        .withColumn("payment_type", mapping_func3[col("payment_type_id")]) \
                        .groupBy("payment_type_id", "payment_type") \
                        .count() \
                        .orderBy(col("payment_type_id").asc()) \
                        .drop("count") 

    # Construct passenger count dimension table 
    dim_passenger_count = df.groupBy(col("passenger_count").alias("passenger_count")) \
                            .count() \
                            .orderBy(col("passenger_count").asc()) \
                            .withColumn("passenger_count_id", monotonically_increasing_id()) \
                            .drop("count")

    # Construct fact table from all other tables
    fact_trips = df.withColumnRenamed("passenger_count", "passenger_count_id") \
                .join(dim_datetime, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'], how="inner") \
                .select("trip_id", "passenger_count_id", "vendor_id", "rate_code_id", "payment_type_id", 
                        "datetime_id", "pick_up_location_id", "drop_off_location_id", "trip_distance",
                        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount", "congestion_surcharge", "airport_fee", "store_and_fwd_flag", "report_date")

    fact_trips = fact_trips.coalesce(5)

    return {"dim_datetime":dim_datetime.toPandas().to_dict(orient="dict"),
    "dim_rate_code":dim_rate_code.toPandas().to_dict(orient="dict"),
    "dim_vendor":dim_vendor.toPandas().to_dict(orient="dict"),
    "dim_payment_type":dim_payment_type.toPandas().to_dict(orient="dict"),
    "dim_passenger_count":dim_passenger_count.toPandas().to_dict(orient="dict"),
    "fact_trips":fact_trips.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
