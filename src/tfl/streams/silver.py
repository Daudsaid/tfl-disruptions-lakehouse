"""Silver: parse bronze JSON, validate, explode statuses, dedupe via MERGE, quarantine bad rows."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from tfl.common.config import Settings
from tfl.common.spark import get_spark

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("line_id", StringType()),
        StructField("line_name", StringType()),
        StructField("polled_at", StringType()),
        StructField(
            "statuses",
            ArrayType(
                StructType(
                    [
                        StructField("severity", IntegerType()),
                        StructField("description", StringType()),
                        StructField("reason", StringType()),
                    ]
                )
            ),
        ),
    ]
)
GOOD_SERVICE = 10


def parse_bronze(bronze_df: DataFrame) -> DataFrame:
    """Parse raw JSON and flag each row as valid or not. Never drops rows."""
    parsed = bronze_df.withColumn("e", F.from_json("raw_value", EVENT_SCHEMA)).withColumn(
        "polled_at_ts", F.to_timestamp("e.polled_at", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    is_valid = (
        F.col("e.event_id").isNotNull()
        & F.col("e.line_id").isNotNull()
        & F.col("polled_at_ts").isNotNull()
        & (F.size("e.statuses") > 0)
        & F.forall("e.statuses", lambda s: s["severity"].between(0, 20))
    )
    return parsed.withColumn("is_valid", F.coalesce(is_valid, F.lit(False)))


def to_silver(valid_df: DataFrame) -> DataFrame:
    """One row per line, per poll, per status."""
    return (
        valid_df.select("*", F.posexplode("e.statuses").alias("status_position", "s"))
        .select(
            F.col("e.event_id").alias("event_id"),
            F.col("e.line_id").alias("line_id"),
            F.col("e.line_name").alias("line_name"),
            F.col("polled_at_ts").alias("polled_at"),
            "status_position",
            F.col("s.severity").alias("status_severity"),
            F.col("s.description").alias("status_description"),
            F.col("s.reason").alias("reason"),
            (F.col("s.severity") != GOOD_SERVICE).alias("is_disrupted"),
            F.col("e.schema_version").alias("schema_version"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("_processed_at"),
            F.to_date("polled_at_ts").alias("poll_date"),
        )
        .dropDuplicates(["event_id", "status_position"])
    )


def upsert_silver(df: DataFrame, path: str) -> None:
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("poll_date").save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(
            df.alias("s"),
            "t.poll_date = s.poll_date AND t.event_id = s.event_id AND t.status_position = s.status_position",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    settings = Settings()
    spark = get_spark("tfl-silver")
    lake = settings.lake_root

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        parsed = parse_bronze(batch_df).persist()

        bad = parsed.filter(~F.col("is_valid"))
        if not bad.isEmpty():
            (
                bad.select("kafka_key", "raw_value", "partition", "offset", "kafka_timestamp", "_ingested_at")
                .withColumn("_quarantined_at", F.current_timestamp())
                .write.format("delta")
                .mode("append")
                .save(f"{lake}/quarantine/line_status")
            )
            (
                bad.select(
                    F.col("kafka_key").alias("key"),
                    F.to_json(F.struct("raw_value", "partition", "offset")).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
                .option("topic", settings.topic_dlq)
                .save()
            )

        upsert_silver(to_silver(parsed.filter("is_valid")), f"{lake}/silver/line_status")
        parsed.unpersist()

    query = (
        spark.readStream.format("delta")
        .load(f"{lake}/bronze/line_status")
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{settings.checkpoint_root}/silver_line_status")
        .trigger(processingTime="1 minute")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
