from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark – countDistinct()").master("local[*]").getOrCreate()

# In PySpark, you can use distinct().count() of DataFrame or countDistinct() SQL function to get the count distinct.
# distinct() eliminates duplicate records(matching all columns of a Row) from DataFrame, count() returns the count of records on DataFrame.
# By chaining these you can get the count distinct of PySpark DataFrame.
# countDistinct() is a SQL function that could be used to get the count distinct of the selected columns.

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data=data,schema=columns)
# df.show()

# Using DataFrame distinct() and count()
# On the above DataFrame, we have a total of 10 rows and one row with all values duplicated,
# performing distinct count ( distinct().count() ) on this DataFrame should get us 9.

# Using SQL to get Count Distinct

# df.createOrReplaceTempView("EMP")
# spark.sql("select distinct(count(*)) from EMP").show()

print("Distinct Count of Department & Salary: "+ str(df2.collect()[0][0]))


