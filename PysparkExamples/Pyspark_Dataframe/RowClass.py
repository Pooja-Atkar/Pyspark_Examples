from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

spark = SparkSession.builder.appName("Row Class").master("local[*]").getOrCreate()

# Row class extends the tuple hence it takes variable number of arguments, Row() is used to create the row object.
# Once the row object created, we can retrieve the data from Row using index similar to tuple.
#  Row class is available by importing pyspark.sql.Row which is represented as a record/row in DataFrame.

# 1. Create a Row Object
# row = Row("james",40)
# print(row[0] + "," + str(row[1]))

# Alternatively you can also write with named arguments.

# row = Row(name="Alice",age=11)
# print(row.name)

# 2. Create Custom Class from Row
# Person = Row("name","age")
# p1=Person("James",40)
# p2=Person("Alice",35)
# print(p1.name + "," + p2.name)

# 3. Using Row class on PySpark RDD
data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]

# #RDD Example 1

# rdd=spark.sparkContext.parallelize(data)
# print(rdd.collect())
# collData = rdd.collect()
# for row in collData:
#     print(row.name + "," +str(row.lang))

# Alternatively, you can also do by creating a Row like class “Person”.

# RDD Example 2
Person=Row("name","lang","state")
data = [Person("James,,Smith",["Java","Scala","C++"],"CA"),
    Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
    Person("Robert,,Williams",["CSharp","VB"],"NV")]

# rdd = spark.sparkContext.parallelize(data)
# collData = rdd.collect()
# print(collData)
# for person in collData:
#     print(person.name + "," +str(person.lang))

# DataFrame Example 1
# columns = ["name","languagesAtSchool","currentState"]
# df = spark.createDataFrame(data)
# df.printSchema()
# df.show()
# collData = df.collect()
# print(collData)
# for row in collData:
#     print(row.name + "," +str(row.lang))

# DataFrame Example 2
# columns = ["name","languagesAtSchool","currentState"]
# df=spark.createDataFrame(data).toDF(*columns)
# df.printSchema()
