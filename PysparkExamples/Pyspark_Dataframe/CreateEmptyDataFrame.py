from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Empty Dataframe Examples").master("local[*]").getOrCreate()

# 1. Create Empty RDD in PySpark
# Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
# print(emptyRDD)

# Creates Empty RDD using parallelize
# rdd2 = spark.sparkContext.parallelize([])
# print(rdd2)

# 2. Create Empty DataFrame with Schema (StructType)
# #Create Schema
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

# Create empty DataFrame from empty RDD
# df = spark.createDataFrame(emptyRDD,schema)
# df.printSchema()

# 3. Convert Empty RDD to DataFrame
# Convert empty RDD to Dataframe
# df1 = emptyRDD.toDF(schema)
# df1.printSchema()

# 4. Create Empty DataFrame with Schema.
#Create empty DataFrame directly.
# df2 = spark.createDataFrame([],schema)
# df2.printSchema()

# 5. Create Empty DataFrame without Schema (no columns)
# Create empty DatFrame with no schema (no columns)
# df3 = spark.createDataFrame([],StructType([]))
# df3.printSchema()