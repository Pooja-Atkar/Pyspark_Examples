from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Convert RDD to DataFrame").master("local[*]").getOrCreate()

# In PySpark, toDF() function of the RDD is used to convert RDD to DataFrame.
# We would need to convert RDD to DataFrame as DataFrame provides more advantages over RDD.
# For instance, DataFrame is a distributed collection of data organized into named columns
# similar to Database tables and provides optimization and performance improvements.

# 1. Create PySpark RDD
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
# print(rdd.collect())

# 2. Convert PySpark RDD to DataFrame
# Converting PySpark RDD to DataFrame can be done using toDF(), createDataFrame().

# 2.1 Using rdd.toDF() function
# PySpark provides toDF() function in RDD which can be used to convert RDD into Dataframe.
# Without Column Name
# df = rdd.toDF()
# df.printSchema()
# df.show(truncate=False)

# With Column Name
deptColumns = ["dept_name","dept_id"]
# df2 = rdd.toDF(deptColumns)
# df2.printSchema()
# df2.show(truncate=

# 2.2 Using PySpark createDataFrame() function

# deptDF = spark.createDataFrame(rdd, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)

# 2.3 Using createDataFrame() with StructType schema
# deptSchema = StructType([
#     StructField('dept_name', StringType(), True),
#     StructField('dept_id', StringType(), True)
# ])

# deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
# deptDF1.printSchema()
# deptDF1.show(truncate=False)
