from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Expression function").master("local[*]").getOrCreate()

# PySpark expr() is a SQL function to execute SQL-like expressions and to use an existing DataFrame column value as an expression argument to Pyspark built-in functions.
# Most of the commonly used SQL functions are either part of the PySpark Column class or built-in pyspark.sql.functions API,
# besides these PySpark also supports many other SQL functions,
# so in order to use these, you have to use expr() function.

# 1. PySpark expr() Syntax
# Following is syntax of the expr() function.
# expr(str)

# expr() function takes SQL expression as a string argument, executes the expression, and returns a PySpark Column type.
# Expressions provided with this function are not a compile-time safety like DataFrame operations.

# 2. PySpark SQL expr() Function Examples
# 2.1 Concatenate Columns using || (similar to SQL)
# using || to concatenate values from two string columns, you can use expr() expression to do exactly same.


#Concatenate columns using || (sql like)
# data=[("James","Bond"),("Scott","Varsa")]
# df=spark.createDataFrame(data).toDF("col1","col2")
# df.withColumn("Name",expr(" col1 ||','|| col2")).show()

# 2.2 Using SQL CASE WHEN with expr()
# PySpark doesn’t have SQL Like CASE WHEN so in order to use this on PySpark DataFrame withColumn() or select(),
# you should use expr() function

# Here, I have used CASE WHEN expression on withColumn() by using expr(),
# this example updates an existing column gender with the derived values,
# M for male, F for Female, and unknown for others.

# data = [("James","M"),("Michael","F"),("Jen","")]
# columns = ["name","gender"]
# df = spark.createDataFrame(data = data, schema = columns)

# #Using CASE WHEN similar to SQL.
# df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
#            "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))

# df2.show()

# 2.3 Using an Existing Column Value for Expression
# Most of the PySpark function takes constant literal values but sometimes we need to use a value from an existing column instead of a constant and this is not possible without expr() expression.
# The below example adds a number of months from an existing column instead of a Python constant.

data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df=spark.createDataFrame(data).toDF("date","increment")

#Add Month value from another column
# df.select(df.date,df.increment,
#      expr("add_months(date,increment)").alias("inc_date")).show()

# 2.4 Giving Column Alias along with expr()
# You can also use SQL like syntax to provide the alias name to the column expression.
# # Providing alias using 'as'

# df.select(df.date,df.increment,
#      expr("""add_months(date,increment) as inc_date""")).show()
# This yields same output as above

# 2.5 Case Function with expr()
# Below example converts long data type to String type.
# Using Cast() Function
# df.select("increment",expr("cast(increment as string) as str_increment")).printSchema()

# 2.7 Arithmetic operations
# expr() is also used to provide arithmetic operations, below examples add value 5 to increment and creates a new column new_increment
# # Arthemetic operations
# df.select(df.date,df.increment,
#      expr("increment + 5 as new_increment")).show()

# 2.8 Using Filter with expr()
# Filter the DataFrame rows can done using expr() expression.
# Use expr()  to filter the rows
# data=[(100,2),(200,3000),(500,500)]
# df=spark.createDataFrame(data).toDF("col1","col2")
# df.filter(expr("col1 == col2")).show()