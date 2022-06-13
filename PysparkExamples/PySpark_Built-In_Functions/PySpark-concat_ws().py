from pyspark.sql import *
from pyspark.sql.functions import col, concat_ws

spark = SparkSession.builder.appName("Pyspark concat_ws() function").master("local[*]").getOrCreate()

# convert an array of String column on DataFrame to a String column (separated or concatenated
# with a comma, space, or any delimiter character) using PySpark function concat_ws() (translates to concat with separator)
# When curating data on DataFrame we may want to convert the Dataframe with complex struct datatypes, arrays and maps to a flat structure.
# here we will see how to convert array type to string type.

columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.printSchema()
# df.show(truncate=False)

# Convert an array of String to String column using concat_ws()
# In order to convert array to a string, PySpark SQL provides a built-in function concat_ws()
# which takes delimiter of your choice as a first argument and array column (type Column) as the second argument.

# Syntax
# concat_ws(sep, *cols)

# In order to use concat_ws() function, you need to import it using pyspark.sql.functions.concat_ws
# Since this function takes the Column type as a second argument, you need to use col().

df2 = df.withColumn("languagesAtSchool",
   concat_ws(",",col("languagesAtSchool")))
# df2.printSchema()
# df2.show(truncate=False)

# Using PySpark SQL expression
# You can also use concat_ws() function with SQL expression.

# df.createOrReplaceTempView("ARRAY_STRING")
# spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool," + \
#     " currentState from ARRAY_STRING").show(truncate=False)
