from concurrent.futures import thread

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Aggregate Functions").master("local[*]").getOrCreate()

# PySpark provides built-in standard Aggregate functions defines in DataFrame API, these come in handy
# when we need to make aggregate operations on DataFrame columns. Aggregate functions operate on a group of
# rows and calculate a single return value for every group.
# All these aggregate functions accept input as, Column type or column name in a string and several other
# arguments based on the function and return Column type.
# When possible try to leverage standard library as they are little bit more compile-time safety, handles null
# and perform better when compared to UDF’s. If your application is critical on performance try to avoid
# using custom UDF at all costs as these are not guarantee on performance.

# PySpark Aggregate Functions
# PySpark SQL Aggregate functions are grouped as “agg_funcs” in Pyspark.

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show()

# approx_count_distinct Aggregate Function
# In PySpark approx_count_distinct() function returns the count of distinct items in a group.

# approx_count_distinct()
# print("approx_count_distinct: " + \
#       str(df.select(approx_count_distinct("salary")).collect()[0][0]))

# Prints approx_count_distinct: 6

# avg (average) Aggregate Function
# avg() function returns the average of values in the input column.

# avg
# print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
# Prints avg: 3400.0

# collect_list Aggregate Function
# collect_list() function returns all values from an input column with duplicates.
# collect_list
# df.select(collect_list("salary")).show(truncate=False)

# collect_set Aggregate Function
# collect_set() function returns all values from an input column with duplicate values eliminated.
# collect_set
# df.select(collect_set("salary")).show(truncate=False)

# count function
# count() function returns number of elements in a column.

# print("count: "+str(df.select(count("salary")).collect()[0]))
# Prints county: 10

# grouping function
# grouping() Indicates whether a given input column is aggregated or not.
# returns 1 for aggregated or 0 for not aggregated in the result.

# first function
# first() function returns the first element in a column when ignoreNulls is set to true, it returns the first non-null element.
# df.select(first("salary")).show()

# last function
# last() function returns the last element in a column. when ignoreNulls is set to true, it returns the last non-null element.

# df.select(last("salary")).show(truncate=False)

# kurtosis function
# kurtosis() function returns the kurtosis of the values in a group.
# df.select(kurtosis("salary")).show(truncate=False)

# max function
# max() function returns the maximum value in a column.
# df.select(max("salary")).show(truncate=False)

# min function
# df.select(min("salary")).show(truncate=False)

# mean function
# mean() function returns the average of the values in a column. Alias for Avg
# df.select(mean("salary")).show(truncate=False)

# skewness function
# skewness() function returns the skewness of the values in a group.
# df.select(skewness("salary")).show(truncate=False)

# stddev(), stddev_samp() and stddev_pop()
# stddev() alias for stddev_samp.
# stddev_samp() function returns the sample standard deviation of values in a column.
# stddev_pop() function returns the population standard deviation of the values in a column.

# df.select(stddev("salary"), stddev_samp("salary"), \
#     stddev_pop("salary")).show(truncate=False)

# sum function
# sum() function Returns the sum of all values in a column.
# df.select(sum("salary")).show()

# variance(), var_samp(), var_pop()
# variance() alias for var_samp
# var_samp() function returns the unbiased variance of the values in a column.
# var_pop() function returns the population variance of the values in a column.

df.select(variance("salary"),var_samp("salary"),var_pop("salary")) \
  .show(truncate=False)