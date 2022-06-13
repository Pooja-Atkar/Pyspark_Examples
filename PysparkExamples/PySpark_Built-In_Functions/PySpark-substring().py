from pyspark.sql import *
from pyspark.sql.functions import substring, col

spark = SparkSession.builder.appName("Substring() function").master("local[*]").getOrCreate()

# In PySpark, the substring() function is used to extract the substring from a DataFrame
# string column by providing the position and length of the string you wanted to extract.

# substring of a column using substring() from pyspark.sql.functions and using substr() from pyspark.sql.Column type.

# Using SQL function substring()
# Using the substring() function of pyspark.sql.functions module we can extract a substring or
# slice of a string from the DataFrame column by providing the position and length of the string you wanted to slice.

# substring(str, pos, len)
# Note: Please note that the position is not zero based, but 1 based index.

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)
df.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2)).show()
# df.printSchema()
# df.show(truncate=False)

# 2. Using substring() with select()
# In Pyspark we can get substring() of a column using select.

# df.select('date', substring('date', 1,4).alias('year'), \
#                   substring('date', 5,2).alias('month'), \
#                   substring('date', 7,2).alias('day')).show()

# 3.Using substring() with selectExpr()
# using selectExpr to get sub string of column(date) as year,month,day.
# df.selectExpr('date', 'substring(date, 1,4) as year', \
#                   'substring(date, 5,2) as month', \
#                  'substring(date, 7,2) as day').show()

# 4. Using substr() from Column type
# Below is the example of getting substring using substr() function from pyspark.sql.Column type in Pyspark.

# df3=df.withColumn('year', col('date').substr(1, 4))\
#   .withColumn('month',col('date').substr(5, 2))\
#   .withColumn('day', col('date').substr(7, 2)).show()