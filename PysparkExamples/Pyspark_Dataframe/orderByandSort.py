from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("OrderBy and Sort Example").getOrCreate()

# You can use either sort() or orderBy() function of PySpark DataFrame
# to sort DataFrame by ascending or descending order based on single or multiple columns,
# you can also do sorting using PySpark SQL sorting functions.

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
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
# df.printSchema()
# df.show(truncate=False)

# DataFrame sorting using the sort() function
# PySpark DataFrame class provides sort() function to sort on one or more columns.
# By default, it sorts by ascending order.

# df.sort("department","state").show(truncate=False)
# df.sort(col("department"),col("state")).show(truncate=False)

# DataFrame sorting using orderBy() function
# PySpark DataFrame also provides orderBy() function to sort on one or more columns.
# By default, it orders by ascending.

# df.orderBy("department","state").show(truncate=False)
# df.orderBy(col("department"),col("state")).show(truncate=False)

# Sort by Ascending (ASC)
# If you wanted to specify the ascending order/sort explicitly on DataFrame,
# you can use the asc method of the Column function.

# df.sort(df.department.asc(),df.state.asc()).show(truncate=False)

# df.sort(col("department").asc(),col("state").asc()).show(truncate=False)

# df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

# Sort by Descending (DESC)
# If you wanted to specify the sorting by descending order on DataFrame, you can use the desc method of the Column function.

# df.sort(df.department.asc(),df.state.desc()).show(truncate=False)

# df.sort(col("department").asc(),col("state").desc()).show(truncate=False)

# df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

# Besides asc() and desc() functions, PySpark also provides asc_nulls_first() and asc_nulls_last() and equivalent descending functions.
# Using Raw SQL
# df.createOrReplaceTempView("EMP")
# spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)


# PySpark DataFrame groupBy(), filter(), and sort() – In this PySpark example, let’s see how to do the following operations in sequence
# 1) DataFrame group by using aggregate function sum(),
# 2) filter() the group by result, and 3) sort() or orderBy() to do descending or ascending order.

# Using DataFrame groupBy(), filter() and sort()
# Below is a complete PySpark DataFrame example of how to do group by, filter and sort by descending order.

from pyspark.sql.functions import sum, col, desc
# df.groupBy("state") \
#   .agg(sum("salary").alias("sum_salary")) \
#   .filter(col("sum_salary") > 100000)  \
#   .sort(desc("sum_salary")).show()

# Alternatively you can use the following SQL expression to achieve the same result.

# df.createOrReplaceTempView("EMP")
# spark.sql("select state, sum(salary) as sum_salary from EMP " +
#           "group by state having sum_salary > 100000 " +
#           "order by sum_salary desc").show()

# First, let’s do a PySpark groupBy() on Dataframe by using an aggregate function sum("salary"),
# groupBy() returns GroupedData object which contains aggregate functions like sum(), max(), min(), avg(), mean(), count().

# df.groupBy("state").sum("salary").show()

# Group by using by giving alias name.
# from pyspark.sql.functions import sum
# dfGroup=df.groupBy("department") \
#           .agg(sum("bonus").alias("sum_salary")).show()

# Filter after group by
# dfFilter=dfGroup.filter(dfGroup.sum_salary > 100000)
# dfFilter.show()


