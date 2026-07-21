import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)
from pyspark.sql.functions import (col, from_json, avg, count, window, to_timestamp, date_format, concat, lit, current_date, to_date)

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


logging.basicConfig(

    level= logging.INFO,
    format= "%(asctime)s [$(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("glue_streaming")

args = getResolvedOptions(sys.argv, [ 
    "STREAM_ARN",
    "S3_OUTPUT_PATH",
    "S3_CHECKPOINT_PATH",
    "TRIGGER_INTERVAL",
    "WINDOW_DURATION",
    "WATERMARK_DURATION",
    ])

STREAM_ARN = args["STREAM_ARN"]
S3_OUTPUT_PATH = args["S3_OUTPUT_PATH"]
S3_CHECKPOINT_PATH = args["S3_CHECKPOINT_PATH"]
TRIGGER_INTERVAL = args["TRIGGER_INTERVAL"]
WINDOW_DURATION = args["WINDOW_DURATION"]
WATERMARK_DURATION = args["WATERMARK_DURATION"]


sc = SparkContext.getOrCreate()
gluecontext = GlueContext(sc)
spark = gluecontext.spark_session
logger.info("Spark and Glue context initialised successfully")

"""
We define a schema explicitly to help catch any bad data at ingestion similarly avoid 
the overhead of schema inference on every job start
"""
schema = StructType ([
StructField("hour", StringType(), True),
StructField("lat", StringType(), True),
StructField("long", StringType(), True),
StructField("signal", StringType(), True),
StructField("network", StringType(), True),
StructField("operator", StringType(), True),
StructField("status", StringType(), True),
StructField("description", StringType(), True),
StructField("speed", StringType(), True),
StructField("satellites", StringType(), True),
StructField("precission", StringType(), True),
StructField("activity", StringType(), True),
StructField("postal_code", StringType(), True),

])


stream_name = STREAM_ARN.split("/")[-1]
aws_region = STREAM_ARN.split(":")[3]

#Connecting to Kinesis via "spark.readStream"

raw_df = (
spark.readStream
.format("kinesis")
.option("stream_name", stream_name)
.option("endpointURL", f"https://kinesis.{aws_region}.amazonaws.com")
.option("region", aws_region)
.option("startingposition", "TRIM_HORIZON")
.option("kinesis.client.describeShardInterval", "30s")
.load()

)

logger.info("Successfully connected to AWS Kinesis Stream via spark.readStream"
"")

parsed_df= (

    raw_df.select(
        from_json(col("data").cast("string"),schema).alias("parsed"),
        col("approximateArrivalTimestamp").alias("kinesis_arrival_time"),
    )
    .select("parsed.*","kinesis_arrival_time")
)

df_with_timestamp = (
    parsed_df
    .withColumn("event_timestamp", to_timestamp(
        concat(current_date().cast("string"), lit(" "), col("hour")),"yyyy-MM-dd HH:mm:ss",
),)
    .withColumn("partiton_hour", date_format(col("event_timestamp"), "HH"))
    .withColumn("signal",     col("signal").cast("int"))
    .withColumn("lat",        col("lat").cast("double"))
    .withColumn("long",       col("long").cast("double"))
    .withColumn("speed",      col("speed").cast("double"))
    .withColumn("satellites", col("satellites").cast("double"))
    .withColumn("precission", col("precission").cast("double"))
    .withColumnRenamed("precission", "precision")
)

logger.info("Schema parsed and timestamp columns derived")

df_with_watermark = df_with_timestamp.withWatermark("event_timestamp", WATERMARK_DURATION)
logger.info(f"Watermark set to {WATERMARK_DURATION} on event_timestamp.")


signal_strength_df = (
    df_with_watermark
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION),
        "postal_code",
        "operator",
        "partition_hour",
    )
    .agg(
        avg("signal").alias("average_signal")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "operator",
        "partition_hour",
        "average_signal",
    )
)

gps_precision_df = (
    df_with_watermark
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION),
        "provider",
        "partition_hour",
    )
    .agg(
        avg("precision").alias("average_precision"),
        avg("satellites").alias("average_satellites"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "provider",
        "partition_hour",
        "average_precision",
        "average_satellites",
    )
)

status_count_df = (
    df_with_watermark
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION),
        "postal_code",
        "status",
        "partition_hour",
    )
    .agg(
        count("*").alias("status_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "status",
        "partition_hour",
        "status_count",
    )
)

logger.info("Aggregation DataFrames defined. Starting write streams.")

# writing stream Output to S3.
# We will use DataFrame.writeStream to ensure a continous sink(that does to exit after execution)
# Spark manages thus sink for the lifetime of the job

# Since we will be using partition projection in in Athena Queries,  we ought to ensure 
# that our S3 directory structure match the partition columns and the storage.location.template configured in Athena.

#CHECKPOINTING
#Each stream will get its own checkpoint location that will store
#  - Kinesis Sequence Number
#  - Open windows ( Agreggation state)
#  - Sink State - Micro-batches that have been committed to sink

def start_write_stream(df, name: str, partition_cols: list[str]):
    """
    Attach a write stream to a DataFrame and start it.

    Extracted into a function to avoid repeating the same 7-line block
    3 times. Each call produces an independent streaming query with its
    own checkpoint and S3 output path.

    Parameters
    ----------
    df             : aggregated streaming DataFrame to write
    name           : folder name under aggregations/ and checkpoints/
    partition_cols : list of columns to partition S3 output by
    """
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", f"{S3_CHECKPOINT_PATH}{name}/")
        .option("path",               f"{S3_OUTPUT_PATH}{name}/")
        .partitionBy(*partition_cols)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


stream_signal   = start_write_stream(
    signal_strength_df,
    "signal_strength_by_operator",
    ["partition_hour", "postal_code"],
)

stream_gps      = start_write_stream(
    gps_precision_df,
    "gps_precision_by_provider",
    ["partition_hour", "provider"],
)

stream_status   = start_write_stream(
    status_count_df,
    "status_count",
    ["partition_hour", "postal_code"],
)

logger.info(
    "All 3 streaming queries started:\n"
    f"  signal_strength_by_operator → {S3_OUTPUT_PATH}signal_strength_by_operator/\n"
    f"  gps_precision_by_provider   → {S3_OUTPUT_PATH}gps_precision_by_provider/\n"
    f"  status_count                → {S3_OUTPUT_PATH}status_count/"
)

# awaitAnyTermination() blocks the main thread here, keeping the Glue job
# alive while all three streams run in background threads.

# If ANY stream fails (exception, Kinesis connectivity issue, S3 write error),
# awaitAnyTermination() unblocks and we fall into the except block where we
# can log the failure cleanly. Glue will mark the job as failed and you'll
# see the error in CloudWatch Logs.

try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    logger.error(f"A streaming query failed: {e}", exc_info=True)
    # In production you'd publish to SNS here to alert the on-call team
    raise