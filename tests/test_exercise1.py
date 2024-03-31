import pytest
import pyspark
from pyspark.sql.types import *
from pyspark.testing import assertDataFrameEqual

from google_playstore_ps_project.src.exercise1 import read_data_from_csv

# use pytest fixture to set up temporary csv file
@pytest.fixture
def csv_file(tmp_path):
    file_path = tmp_path / "data.csv"
    data_csv_text = """App,Category,Rating\nInstagram,SOCIAL,4.3"""
    with open(file_path, "w") as file:
        file.write(data_csv_text)
    
    return str(file_path)


def test_read_csv_true(spark_session,
                        csv_file
                    ):
    df_actual = read_data_from_csv(sparksession = spark_session,path = csv_file)
    df_data = [
        ("App", "Category", "Rating"),
        ("Instagram", "SOCIAL", 4.3)
    ]

    df_schema = StructType([
        StructField("_c0", StringType(), True),
        StructField("_c1", StringType(), True),
        StructField("_c2", StringType(), True)
    ])
    df_expected = spark_session.createDataFrame(data = df_data, schema = df_schema)

    assertDataFrameEqual(df_actual, df_expected)

def test_read_csv_assertion_error(spark_session,
                                    csv_file
                                ):
    
    with pytest.raises(AssertionError) as exception_info:
        read_data_from_csv(sparksession = spark_session,
                            path = csv_file,
                            header_flag = "abc",
                            infer_schema = "xyz"
                        )
    assert exception_info.value.args[0] == "infer_schema and header_flag parameter can only take true and flase as its value"

def test_read_csv_header(spark_session,
                         csv_file
                         ):
    df_actual = read_data_from_csv(sparksession = spark_session, path = csv_file, header_flag = "true")
    df_data = [
        ("Instagram", "SOCIAL", 4.3)
    ]

    df_schema = StructType([
        StructField("App", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Rating", StringType(), True)
    ])
    df_expected = spark_session.createDataFrame(data = df_data, schema = df_schema)

    assertDataFrameEqual(df_actual, df_expected)


def test_read_csv_infer_schema(spark_session,
                               csv_file
                               ):
    df_actual = read_data_from_csv(sparksession = spark_session, 
                                   path = csv_file,
                                   header_flag = "true",
                                   infer_schema = "true")
    
    df_data = [
        ("Instagram", "SOCIAL", 4.3)
    ]

    df_schema = StructType([
        StructField("App", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Rating", DoubleType(), True)
    ])
    df_expected = spark_session.createDataFrame(data = df_data, schema = df_schema)

    assertDataFrameEqual(df_actual, df_expected)


def test_read_csv_custom_schema(spark_session,
                                csv_file
                                ):
    custom_schema = StructType([
        StructField("App", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Rating", StringType(), True)
    ])

    df_schema = custom_schema
    
    df_actual = read_data_from_csv(sparksession = spark_session, 
                                   path = csv_file,
                                   header_flag = "true",
                                   infer_schema = "true",
                                   custom_schema = custom_schema)
    
    df_data = [
        ("Instagram", "SOCIAL", 4.3)
    ]

    df_expected = spark_session.createDataFrame(data = df_data, schema = df_schema)

    assertDataFrameEqual(df_actual, df_expected)