from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Filter and Where Example").getOrCreate()

# PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or SQL expression,
# you can also use where() clause instead of the filter() if you are coming from an SQL background,
# both these functions operate exactly the same.

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType()),
        StructField('middlename', StringType()),
        StructField('lastname', StringType())
    ])),
    StructField('languages', ArrayType(StringType())),
    StructField('state', StringType()),
    StructField('gender', StringType())
])

df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show()

# 2. DataFrame filter() with Column Condition
# Using equals condition
# df.filter(df.state == "OH").show(truncate=False)

# not equals condition
# df.filter(df.state != "OH").show(truncate=False)
# df.filter(~(df.state == "OH")).show(truncate=False)

#Using SQL col() function
from pyspark.sql.functions import col
# df.filter(col("state") == "OH").show(truncate=False)

# 3. DataFrame filter() with SQL Expression
# Using SQL Expression
# df.filter("gender == 'M'").show()
# For not equal
# df.filter("gender != 'M'").show()
# df.filter("gender <> 'M'").show()

# 4. PySpark Filter with Multiple Conditions
# Filter multiple condition
# df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)

# 5. Filter Based on List Values
#Filter IS IN List values
li=["OH","CA","DE"]
# df.filter(df.state.isin(li)).show()

# Filter NOT IS IN List values
#These show all records with NY (NY is not part of the list)
# df.filter(~df.state.isin(li)).show()
# df.filter(df.state.isin(li)==False).show()

# 6. Filter Based on Starts With, Ends With, Contains
# Using startswith
# df.filter(df.state.startswith("N")).show()
# using endswith
# df.filter(df.state.endswith("H")).show()
# contains
# df.filter(df.state.contains("H")).show()

# 7. PySpark Filter like and rlike
data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
# df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
# df2.filter(df2.name.rlike("(?i)^*rose$")).show()

# 8. Filter on an Array column

from pyspark.sql.functions import array_contains
# df.filter(array_contains(df.languages,"Java")).show(truncate=False)

# 9. Filtering on Nested Struct columns
# Struct condition
# df.filter(df.name.lastname == "Williams").show(truncate=False)
