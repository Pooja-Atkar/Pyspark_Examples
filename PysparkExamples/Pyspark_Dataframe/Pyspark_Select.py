from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Select Examples').master("local[*]").getOrCreate()

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
# df.show(truncate=False)

# 1. Select Single & Multiple Columns From PySpark.

# df.select("firstname","lastname").show()
# df.select(df.firstname,df.lastname).show()
# df.select(df["firstname"],df["lastname"]).show()

# By using col() function
# df.select(col("firstname"),col("lastname")).show()

# Select columns by regular expression
# df.select(df.colRegex("`^.*name*`")).show()

#  2. Select All Columns From List

# Select All columns from List
# df.select(*columns).show()

# Select All columns
# df.select([col for col in df.columns]).show()
# df.select("*").show()

# 3. Select Columns by Index

# Selects first 3 columns and top 3 rows
# df.select(df.columns[:3]).show(3)

# Selects columns 2 to 4  and top 3 rows
# df.select(df.columns[2:4]).show(3)

# 4. Select Nested Struct Columns from PySpark
data1 = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

schema1 = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType()),
         StructField('middlename', StringType()),
         StructField('lastname', StringType())
         ])),
     StructField('state', StringType()),
     StructField('gender', StringType())
     ])
df2 = spark.createDataFrame(data = data1, schema = schema1)
# df2.printSchema()
# df2.show(truncate=False) # shows all columns

# Now, let’s select struct column.This returns struct column name as is.
# df2.select("name").show(truncate=False)

# In order to select the specific column from a nested struct, you need to explicitly qualify the nested struct column name.
# df2.select("name.firstname","name.lastname").show(truncate=False)

# In order to get all columns from struct column.
# df2.select("name.*").show(truncate=False)
