import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

POSTGRES_JDBC = "org.postgresql:postgresql:42.7.4"


def get_spark(app_name: str) -> SparkSession:
    """Local Spark with Delta Lake, the Kafka connector and the Postgres JDBC driver."""
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.ivy", "/root/.ivy2")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[kafka_pkg, POSTGRES_JDBC]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
