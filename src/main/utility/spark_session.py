# import  os
# from pyspark.sql import SparkSession
# from src.main.utility.logging_config import logger
# from pyspark.sql import *
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from src.main.utility.logging_config import *
#
#
# def spark_session():
#     spark = SparkSession.builder.master("spark://localhost:7077") \
#         .appName("myproject")\
#         .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
#         .getOrCreate()
#     logger.info("spark session %s",spark)
#     return spark
#



import os
from pyspark.sql import SparkSession

# def spark_session():
#
#     spark = SparkSession.builder \
#         .master("local[*]") \
#         .appName("de_project") \
#         .getOrCreate()
#
#     return spark
#
# import os
# from pyspark.sql import SparkSession
#
# def spark_session():
#
#     os.environ["PYSPARK_PYTHON"] = r"C:\Users\firoj\Music\spark_data\de_project\golu.venv\Scripts\python.exe"
#     os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\firoj\Music\spark_data\de_project\golu.venv\Scripts\python.exe"
#
#     spark = SparkSession.builder \
#         .master("local[*]") \
#         .appName("de_project") \
#         .getOrCreate()
#
#     return spark


import os
from pyspark.sql import SparkSession

def spark_session():

    os.environ["PYSPARK_PYTHON"] = r"C:\Users\firoj\Music\spark_data\de_project\golu.venv\Scripts\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\firoj\Music\spark_data\de_project\golu.venv\Scripts\python.exe"

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("de_project") \
        .config(
            "spark.jars",
            r"C:\Users\firoj\Music\spark_data\mysql-connector-j-9.7.0\mysql-connector-j-9.7.0.jar"
        ) \
        .getOrCreate()

    return spark