from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark – explode").master("local[*]").getOrCreate()

# PySpark explode function can be used to explode an Array of Array (nested Array) ArrayType(ArrayType(StringType))
# columns to rows on PySpark DataFrame using python example.

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
# df.printSchema()
# df.show(truncate=False)

from pyspark.sql.functions import explode
# df.select(df.name,explode(df.subjects)).show(truncate=False)

# If you want to flatten the arrays, use flatten function which converts array of array columns to a single array on DataFrame.

# from pyspark.sql.functions import flatten
# df.select(df.name,flatten(df.subjects)).show(truncate=False)
