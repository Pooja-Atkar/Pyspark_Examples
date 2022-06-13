from pyspark.sql import *
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("Pyspark split() function").master("local[*]").getOrCreate()

# PySpark SQL provides split() function to convert delimiter separated String to an Array (StringType to ArrayType) column on DataFrame.
# This can be done by splitting a string column based on a delimiter like space, comma, pipe e.t.c, and converting it into ArrayType.

# Split() function syntax
# pyspark.sql.functions.split(str, pattern, limit=-1)
# The split() function takes the first argument as the DataFrame column of type String and the second argument string delimiter that you want to split on.
# You can also use the pattern as a delimiter. This function returns pyspark.sql.Column of type Array.

data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.printSchema()

df2 = df.select(split(col("name"),",").alias("NameArray")).drop("name")
df2.printSchema()
df2.show()

# Convert String to Array Column using SQL Query
# Since PySpark provides a way to execute the raw SQL, let’s learn how to write the same example using Spark SQL expression.
# In order to use raw SQL, first, you need to create a table using createOrReplaceTempView().
# This creates a temporary view from the Dataframe and this view is available lifetime of the current Spark context.

# df.createOrReplaceTempView("PERSON")
# spark.sql("select SPLIT(name,',') as NameArray from PERSON").show()

