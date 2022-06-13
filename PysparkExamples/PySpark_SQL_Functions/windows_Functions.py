from pyspark.sql import *

spark = SparkSession.builder.appName("Windows Functions").master("local[*]").getOrCreate()

# PySpark Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows.

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)

# 2. PySpark Window Ranking functions
# 2.1 row_number Window Function
# row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

# df.withColumn("row_number",row_number().over(windowSpec)) \
#     .show(truncate=False)

# 2.2 rank Window Function
# rank() window function is used to provide a rank to the result within a window partition.
# This function leaves gaps in rank when there are ties.

"""rank"""
from pyspark.sql.functions import rank
# df.withColumn("rank",rank().over(windowSpec)).show()

# 2.3 dense_rank Window Function
# dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
# This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

"""dens_rank"""
from pyspark.sql.functions import dense_rank
# df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()

# 2.4 percent_rank Window Function
""" percent_rank """
from pyspark.sql.functions import percent_rank
# df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()

# 2.5 ntile Window Function
# ntile() window function returns the relative rank of result rows within a window partition.
# In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)

"""ntile"""
from pyspark.sql.functions import ntile
# df.withColumn("ntile",ntile(2).over(windowSpec)).show()

# 3. PySpark Window Analytic functions
# 3.1 cume_dist Window Function
# cume_dist() window function is used to get the cumulative distribution of values within a window partition.
# This is the same as the DENSE_RANK function in SQL.

""" cume_dist """
from pyspark.sql.functions import cume_dist
# df.withColumn("cume_dist",cume_dist().over(windowSpec)).show()

# 3.2 lag Window Function
# This is the same as the LAG function in SQL.

"""lag"""
from pyspark.sql.functions import lag
# df.withColumn("lag",lag("salary",2).over(windowSpec)).show()

# 3.3 lead Window Function
# This is the same as the LEAD function in SQL.

"""lead"""
from pyspark.sql.functions import lead
# df.withColumn("lead",lead("salary",2).over(windowSpec)).show()

# 4. PySpark Window Aggregate Functions

# windowSpecAgg  = Window.partitionBy("department")
# from pyspark.sql.functions import col,avg,sum,min,max,row_number
# df.withColumn("row",row_number().over(windowSpec)) \
#   .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
#   .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
#   .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
#   .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
#   .where(col("row")==1).select("department","avg","sum","min","max") \
#   .show()
