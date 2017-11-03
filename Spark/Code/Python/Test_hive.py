#Simple Example to integrate Spark Sql and Hive.Please ensure Spark,Hive version.
# The hive-site.xml should be copied to spark conf path$SPARK_HOME/conf and the mysql-connect-java-*.jar to your $SPARK_HOME/jars.
"""
A simple example demonstrating Spark SQL Hive integration.
Run with:
  spark-submit Test_hive.py
"""
#from pyspark.conf import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# warehouse_location points to the default location for managed databases and tables

>>> warehouse_location = abspath('spark-warehouse')

>>> spark = SparkSession.builder.appName("Python Spark SQL Hive integration example")\
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
	
#create SparkContext and SQLContext, HiveContext
>>> sc = SparkContext()
>>> sqlContext = SQLContext(sc)
>>> hive_context = HiveContext(sc)
>>> df = hive_context.table("databaseName.tableName").show()
# +---+-------+
# |sno|  Sal  |
# +---+-------+
# |1  |val_001|
# |2  |val_002|
# |3  |val_003|


# Queries are expressed in HiveQL
spark.sql("SELECT * FROM tableName").show()

# The results of SQL queries are themselves DataFrames and support all normal functions.
