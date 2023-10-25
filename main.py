import os
import sys
import traceback

# Path for spark source folder
os.environ['SPARK_HOME'] = "C:\\Spark\\spark-3.5.0-bin-hadoop3"

# Append pyspark  to Python Path
sys.path.append("C:\\Spark\\spark-3.5.0-bin-hadoop3\\python")
sys.path.append("C:\\Spark\\spark-3.5.0-bin-hadoop3\\python\\lib\\py4j-0.10.9.7-src")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.\
        appName('Anmv1').\
        config("spark.jars", "mysql-connector-java-8.0.29.jar").\
        getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/testingsystem3") \
        .option("dbtable", "shoping") \
        .option("user", "root") \
        .option("password", "root") \
        .load()

    df.show()

    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules {}".format(traceback.format_exc()))
sys.exit(1)
