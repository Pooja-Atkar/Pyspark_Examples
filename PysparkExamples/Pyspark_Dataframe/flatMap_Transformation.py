from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("flatMap Transformation Example").getOrCreate()

# PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns)
# after applying the function on every element and returns a new PySpark RDD/DataFrame.

# create an RDD from the list.
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
# for element in rdd.collect():
#     print(element)

# flatMap() Syntax
# flatMap(f, preservesPartitioning=False)
# flatMap() Example

rdd2=rdd.flatMap(lambda x: x.split(" "))
# for element in rdd2.collect():
#     print(element)

# Using flatMap() transformation on DataFrame
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df2 = df.select(df.name,explode(df.knownLanguages))
# df2.printSchema()
# df2.show()


# foreach()

# Using foreach() to Loop Through Rows in DataFrame
# Similar to map(), foreach() also applied to every row of DataFrame,
# the difference being foreach() is an action and it returns nothing. Below are some examples to iterate through DataFrame using for each.

# Foreach example
# def f(x): print(x)
# df.foreach(f)

data = [('James','Smith','M',30),('Anna','Rose','F',41),
  ('Robert','Williams','M',62)]
columns = ["firstname","lastname","gender","salary"]
# df = spark.createDataFrame(data=data, schema = columns)
# df.show()

#  Another example
# df.foreach(lambda x:
#     print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2)))
