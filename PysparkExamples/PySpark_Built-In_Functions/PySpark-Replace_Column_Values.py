from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Replace Column Values in DataFrame").master("local[*]").getOrCreate()

# You can replace column values of PySpark DataFrame by using SQL string
# functions regexp_replace(), translate(), and overlay() with Python examples.

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
# df.show()

# 1. PySpark Replace String Column Values
# By using PySpark SQL function regexp_replace() you can replace a column value with a string for another string/substring.
# regexp_replace() uses Java regex for matching,
# if the regex does not match it returns an empty string,
# the below example replace the street name Rd value with Road string on address column.

#Replace part of string with another string
from pyspark.sql.functions import regexp_replace
df.withColumn('address', regexp_replace('address', 'Rd', 'Road')) \
  .show(truncate=False)

# 2. Replace Column Values Conditionally
# In the above example, we just replaced Rd with Road, but not replaced St and Ave values,
# let’s see how to replace column values conditionally in PySpark Dataframe by using when().otherwise() SQL condition function.


#Replace string column value conditionally
# from pyspark.sql.functions import when
# df.withColumn('address',
#     when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
#    .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
#    .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
#    .otherwise(df.address)) \
#    .show(truncate=False)

# 3. Replace Column Value with Dictionary (map)
# You can also replace column values from the python dictionary (map). In the below example,
# we replace the string value of the state column with the full abbreviated name from a dictionary key-value pair,
# in order to do so I use PySpark map() transformation to loop through each row of DataFrame.

# Replace values from Dictionary

# stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
# df2=df.rdd.map(lambda x:
#     (x.id,x.address,stateDic[x.state])
#     ).toDF(["id","address","state"])
# df2.show()

# 4. Replace Column Value Character by Character
# By using translate() string function you can replace character by character of DataFrame column value. In
# the below example, every character of 1 is replaced with A, 2 replaced with B, and 3 replaced with C on the address column.

#Using translate to replace character by character
# from pyspark.sql.functions import translate
# df.withColumn('address', translate('address', '123', 'ABC')) \
#   .show(truncate=False)

# 5. Replace Column with Another Column Value
# By using expr() and regexp_replace() you can replace column value with a value from another DataFrame column.
# In the below example, we match the value from col2 in col1 and replace with col3 to create new_column.
# Use expr() to provide SQL like expressions and is used to refer to another column to perform operations.

# Replace column with another column

# from pyspark.sql.functions import expr
# df = spark.createDataFrame(
#    [("ABCDE_XYZ", "XYZ","FGH")],
#     ("col1", "col2","col3"))
# df.withColumn("new_column",
#               expr("regexp_replace(col1, col2, col3)")
#               .alias("replaced_value")).show()

# 7. Using overlay() Function
# Replace column value with a string value from another column.

#Overlay
# from pyspark.sql.functions import overlay
# df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
# df.select(overlay("col1", "col2", 7).alias("overlayed")).show()
