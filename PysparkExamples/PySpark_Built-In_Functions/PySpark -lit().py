from pyspark.sql import *
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.appName("pyspark lit() function").master("local[*]").getOrCreate()

# PySpark lit() – Add Literal or Constant to DataFrame
# PySpark SQL functions lit() and typedLit() are used to add a new column to DataFrame by assigning a literal or constant value.
# Both these functions return Column type as return type.
# Both of these are available in PySpark by importing pyspark.sql.functions

data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)

# lit() Function to Add Constant Column
# PySpark lit() function is used to add constant or literal value as a new column to the DataFrame.

# Example 1: Simple usage of lit() function

df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
# df2.show(truncate=False)

# Example 2 : lit() function with withColumn
# example shows how to use pyspark lit() function using withColumn to derive a new column based on some conditions.

# df3 = df2.withColumn("lit_value2", when(col("Salary") >=40000,lit("100")).otherwise(lit("200")))
# df3.show()
