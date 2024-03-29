from pyspark.sql import SparkSession
from typing import List
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def read_data_from_csv(sparksession,
                       path: str,
                       header_flag: str = "false",
                       infer_schema: str = "false",
                       custom_schema = None
                    ):
    """
    This function reads a csv file from the path specified with the necessary modifications
    """
    assert infer_schema in ["true", "false"] and header_flag in ["true", "false"], \
        "infer_schema and header_flag parameter can only take true and flase as its value"
    
    if custom_schema is None:
        df = sparksession \
            .read \
            .format("csv") \
            .option("header", header_flag) \
            .option("inferSchema", infer_schema) \
            .option("escape", '"') \
            .load(path)
    else:
        df = sparksession \
            .read \
            .format("csv") \
            .option("header", header_flag) \
            .schema(custom_schema) \
            .load(path)
    
    return df

def clean_data(sparksession,
               df,
               drop_columns_list: List = []
            ):
    df = df.drop(*drop_columns_list) \
        .withColumn("Reviews", F.col("Reviews").cast(IntegerType())) \
        .withColumn("Installs", F.regexp_replace(F.col("Installs"), "[^0-9]", "")) \
        .withColumn("Installs", F.col("Installs").cast(IntegerType())) \
        .withColumn("Price", F.regexp_replace(F.col("Price"), "[$]", "")) \
        .withColumn("Price", F.col("Price").cast(IntegerType()))
    
    return df


def main():
    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("exercise1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    csv_file_path = "/google_playstore_ps_project/data/googleplaystore.csv"
    columns_to_be_removed = ["Size", "Content Rating", "Last Updated", "Android Ver", "Current Ver"]



    df = read_data_from_csv(sparksession = spark,
                            path = csv_file_path,
                            header_flag = "true",
                            infer_schema = "true"
                        )

    df = clean_data(sparksession = spark,
                    df = df,
                    drop_columns_list = columns_to_be_removed
                )

    print("... printing the schema of dataframe ...")
    df.printSchema()

    print("... printing head of the dataframe ...")
    df.show(5)

    print("... Top 10 apps by number of reviews ...")
    df.groupBy("App") \
        .agg(F.sum("Reviews").alias("Total Reviews")) \
        .orderBy(F.desc(F.col("Total Reviews"))) \
        .show(10)

    print("... Top 10 installed apps by type (paid / free) ...")
    df.groupBy("Type", "App") \
        .agg(F.sum("Installs").alias("Total Installs")) \
        .orderBy(F.desc(F.col("Type")), F.desc(F.col("Total Installs"))) \
        .show()

    print("... Category wise distribution of installed apps ...")
    df.groupBy("Category") \
        .agg(F.sum("Installs").alias("Total Installs")) \
        .orderBy(F.desc(F.col("Total Installs"))) \
        .show(10)

    print("... Top paid apps ...")
    df.groupBy("App") \
        .agg(F.sum("Price").alias("Total Amount")) \
        .orderBy(F.desc(F.col("Total Amount"))) \
        .show(10)

    spark.stop()



if __name__ == "__main__":
    main()