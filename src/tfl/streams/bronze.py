"""Bronze: land every Kafka message in Delta, untouched, with Kafka metadata."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl.common.config import Settings
from tfl.common.spark import get_spark


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
        F.to_date("timestamp").alias("ingest_date"),
    )


def main() -> None:
    settings = Settings()
    spark = get_spark("tfl-bronze")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.topic_line_status)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        to_bronze(kafka_df)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{settings.checkpoint_root}/bronze_line_status")
        .partitionBy("ingest_date")
        .trigger(processingTime="30 seconds")
        .start(f"{settings.lake_root}/bronze/line_status")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
