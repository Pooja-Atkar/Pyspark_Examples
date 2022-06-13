from pyspark.sql import *
from pyspark.sql.functions import when, col, expr

spark = SparkSession.builder.appName("When Function").master("local[*]").getOrCreate()

# PySpark When Otherwise and SQL Case When on DataFrame with Examples – Similar to SQL and programming languages,
# PySpark supports a way to check multiple conditions in sequence and returns a value when the first condition met
# by using SQL like case when and when().otherwise() expressions, these works similar to “Switch" and "if then else"
# statements.

# PySpark When Otherwise – when() is a SQL function that returns a Column type and otherwise() is a function
# of Column, if otherwise() is not used, it returns a None/NULL value.

# PySpark SQL Case When – This is similar to SQL expression, Usage: CASE WHEN cond1 THEN result WHEN cond2 THEN
# result... ELSE result END.

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
# df.show()

# 1. Using when() otherwise() on PySpark DataFrame.
# PySpark when() is SQL function, in order to use this first you should import and this returns a Column type,
# otherwise() is a function of Column, when otherwise() not used and none of the conditions met it assigns
# None (Null) value. Usage would be like when(condition).otherwise(default).

# when() function take 2 parameters, first param takes a condition and second takes a literal value or Column,
# if condition evaluates to true then it returns a value from second param.
# The below code snippet replaces the value of gender with a new derived value, when conditions not matched,
# we are assigning “Unknown” as value, for null assigning empty.

df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
# df2.show()

# Using with select()
df3 = df.select(col("*"),when(df.gender == "M","Male")
                  .when(df.gender == "F","Female")
                  .when(df.gender.isNull() ,"")
                  .otherwise(df.gender).alias("new_gender"))
# df3.show()

# 2.1 Using Case When Else on DataFrame using withColumn() & select()

# Using Case When on withColumn()
df4 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
               "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
               "ELSE gender END"))
# df4.show()

# Using Case When on select()
df5 = df.select(col("*"), expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
           "ELSE gender END").alias("new_gender"))
# df5.show()

# 2.2 Using Case When on SQL Expression
# You can also use Case When with SQL statement after creating a temporary view.

# df.createOrReplaceTempView("EMP")
# spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " +
#                "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#               "ELSE gender END as new_gender from EMP").show()

