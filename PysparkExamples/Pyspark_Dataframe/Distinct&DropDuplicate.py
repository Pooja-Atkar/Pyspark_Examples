from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Distinct to Drop Duplicate Example").getOrCreate()

# PySpark distinct() function is used to drop/remove the duplicate rows (all columns) from DataFrame and dropDuplicates()
# is used to drop rows based on selected (one or multiple) columns.

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show()

# 1. Get Distinct Rows (By Comparing All Columns)
distinctDF = df.distinct()
# print("Distinct count: "+str(distinctDF.count()))
# distinctDF.show(truncate=False)

#Alternatively, you can also run dropDuplicates() function which returns a new DataFrame after removing duplicate rows.

df2 = df.dropDuplicates()
# print("Distinct count: "+str(df2.count()))
# df2.show(truncate=False)

# 2. PySpark Distinct of Selected Multiple Columns
dropDisDF = df.dropDuplicates(["department","salary"])
# print("Distinct count of department & salary : "+str(dropDisDF.count()))
# dropDisDF.show(truncate=False)

