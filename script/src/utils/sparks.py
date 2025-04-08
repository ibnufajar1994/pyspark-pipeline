
from pyspark.sql import SparkSession


def spark_session():
    spark = SparkSession.builder.appName(
        "Pyspark Data Pipeline"
    ).getOrCreate()

    # handle legacy time parser
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    return spark