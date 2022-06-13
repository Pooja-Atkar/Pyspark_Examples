from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("MapType Example").getOrCreate()

# PySpark MapType (also called map type) is a data type to represent Python Dictionary (dict) to store key-value pair,
# a MapType object comprises three fields, keyType (a DataType), valueType (a DataType) and valueContainsNull (a BooleanType).

# What is PySpark MapType
# PySpark MapType is used to represent map key-value pair similar to python Dictionary (Dict),
# it extends DataType class which is a superclass of all types in PySpark and takes two mandatory arguments keyType and
# valueType of type DataType and one optional boolean argument valueContainsNull.
# keyType and valueType can be any type that extends the DataType class.
# for e.g StringType, IntegerType, ArrayType, MapType, StructType (struct) e.t.c.

# 1. Create PySpark MapType
# In order to use MapType data type first, you need to import it from pyspark.sql.types.
# MapType and use MapType() constructor to create a map object.

# from pyspark.sql.types import StringType, MapType
# mapCol = MapType(StringType(),StringType(),False)

# MapType Key Points:

# The First param keyType is used to specify the type of the key in the map.
# The Second param valueType is used to specify the type of the value in the map.
# Third parm valueContainsNull is an optional boolean type that is used to specify
# if the value of the second param can accept Null/None values.
# The key of the map won’t accept None/Null values.
# PySpark provides several SQL functions to work with MapType.

# 2. Create MapType From StructType
# create a MapType by using PySpark StructType & StructField, StructType() constructor takes list of StructField,
# StructField takes a fieldname and type of the value.

from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)])

# create a DataFrame by using above StructType schema.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
# df.printSchema()
# df.show()

# 3. Access PySpark MapType Elements
# how to extract the key and values from the PySpark DataFrame Dictionary column.
# Here I have used PySpark map transformation to read the values of properties (MapType column)

df3=df.rdd.map(lambda x:
    (x.name,x.properties["hair"],x.properties["eye"])).toDF(["name","hair","eye"])
# df3.printSchema()
# df3.show()

# use another way to get the value of a key from Map using getItem() of Column type,
# this method takes a key as an argument and returns a value.


# df.withColumn("hair",df.properties.getItem("hair")) \
#   .withColumn("eye",df.properties.getItem("eye")).drop("properties").show()

# df.withColumn("hair",df.properties["hair"]) \
#   .withColumn("eye",df.properties["eye"]).drop("properties").show()

# 4. Functions
# 4.1 – explode
# from pyspark.sql.functions import explode
# df.select(df.name,explode(df.properties)).show()

# 4.2 map_keys() – Get All Map Keys
# from pyspark.sql.functions import map_keys
# df.select(df.name,map_keys(df.properties)).show()

# In case if you wanted to get all map keys as Python List. WARNING: This runs very slow.

# from pyspark.sql.functions import explode,map_keys
# keysDF = df.select(explode(map_keys(df.properties))).distinct()
# keysList = keysDF.rdd.map(lambda x:x[0]).collect()
# print(keysList)
# o/p-['eye', 'hair']

# 4.3 map_values() – Get All map Values

# from pyspark.sql.functions import map_values
# df.select(df.name,map_values(df.properties)).show()