from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope = "session")
def spark_session():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("testSparkSession") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    yield spark
    spark.stop()

