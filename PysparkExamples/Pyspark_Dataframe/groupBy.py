from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("groupBy Example").getOrCreate()

# Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical
# data into groups on DataFrame and perform aggregate functions on the grouped data.

# When we perform groupBy() on PySpark Dataframe, it returns GroupedData object which contains below aggregate functions.
# count() - Returns the count of rows for each group.
# mean() - Returns the mean of values for each group.
# max() - Returns the maximum of values for each group.
# min() - Returns the minimum of values for each group.
# sum() - Returns the total for values for each group.
# avg() - Returns the average for values for each group.
# agg() - Using agg() function, we can calculate more than one aggregate at a time.


simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show(truncate=False)

# PySpark groupBy and aggregate on DataFrame columns
# Let’s do the groupBy() on department column of DataFrame and then find the sum of salary for each department using sum() aggregate function.
# df.groupBy("department").sum("salary").show(truncate=False)

# we can calculate the number of employee in each department using count()
# df.groupBy("department").count().show()

# Calculate the minimum salary of each department using min()
# df.groupBy("department").min("salary").show()

# Calculate the maximin salary of each department using max()
# df.groupBy("department").max("salary").show()

# Calculate the average salary of each department using avg()
# df.groupBy("department").avg( "salary").show()

# Calculate the mean salary of each department using mean()
# df.groupBy("department").mean( "salary").show()

# PySpark groupBy and aggregate on multiple columns
# Similarly, we can also run groupBy and aggregate on two or more DataFrame columns.

# GroupBy on multiple columns
# df.groupBy("department","state").sum("salary","bonus").show()

# Running more aggregates at a time
# Using agg() aggregate function we can calculate many aggregations at a time on a single statement using
# PySpark SQL aggregate functions sum(), avg(), min(), max() mean() e.t.c. In order to use these,
# we should import "from pyspark.sql.functions import sum,avg,max,min,mean,count"

# df.groupBy("department").agg(sum("salary").alias("sum_salary"),
#          avg("salary").alias("avg_salary"),
#          sum("bonus").alias("sum_bonus"),
#          max("bonus").alias("max_bonus")).show()

# Using filter on aggregate data
# Similar to SQL “HAVING” clause, On PySpark DataFrame we can use either where() or filter() function to filter the rows of aggregated data.
# df.groupBy("department").agg(sum("salary").alias("sum_salary"),
#       avg("salary").alias("avg_salary"),
#       sum("bonus").alias("sum_bonus"),
#       max("bonus").alias("max_bonus")).where(col("sum_bonus") >= 50000).show()