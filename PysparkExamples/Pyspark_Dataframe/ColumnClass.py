from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row

spark = SparkSession.builder.appName("Column Class").master("local[*]").getOrCreate()
# 1. Create Column Class Object
# One of the simplest ways to create a Column class object is by using PySpark lit() SQL function,
# this takes a literal value and returns a Column object.

colobj = lit("Column Object")

# You can also access the Column from DataFrame by multiple ways.

data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data).toDF("name.fname","age")
# df.printSchema()

# Using DataFrame object (df)
# df.select(df.age).show()
# df.select(df["age"]).show()

#Accessing column name with dot (with backticks)
# df.select(df["`name.fname`"]).show()

#Using SQL col() function
# df.select(col("age")).show()

#Accessing column name with dot (with backticks)
# df.select(col("`name.fname`")).show()


#Create DataFrame with struct using Row class
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
# df.printSchema()

#Access struct column
# df.select(df.prop.hair).show()
# df.select(df["prop.hair"]).show()
# df.select(col("prop.hair")).show()

#Access all columns from struct
# df.select(col("prop.*")).show()

# 2. PySpark Column Operators
# PySpark column also provides a way to do arithmetic operations on columns using operators.

data = [(100,2,1),(200,3,4),(300,4,4)]
df = spark.createDataFrame(data).toDF("col1","col2","col3")
#Arthmetic operations
# df.select(df.col1 + df.col2).show()
# df.select(df.col1 - df.col2).show()
# df.select(df.col1 * df.col2).show()
# df.select(df.col1 / df.col2).show()
# df.select(df.col1 % df.col2).show()
#
# df.select(df.col2 > df.col3).show()
# df.select(df.col2 < df.col3).show()
# df.select(df.col2 == df.col3).show()

# 3. PySpark Column Functions.
data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')]
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)

# 4.1 alias() – Set’s name to Column
# df.select(df.fname.alias("first_name"),df.lname.alias("last_name")).show()

# Another Example
# df.select(expr("fname || ',' || lname").alias("fullname")).show()

#4.2 asc() & desc() – Sort the DataFrame columns by Ascending or Descending order.
#asc, desc to sort ascending and descending order repsectively.
# df.sort(df.fname.asc()).show()
# df.sort(df.fname.desc()).show()

#4.3 cast() & astype() – Used to convert the data Type.
#cast
# df.select(df.fname,df.id.cast("int")).printSchema()

#4.4 between() – Returns a Boolean expression when a column values in between lower and upper bound.
#between
# df.filter(df.id.between(100,300)).show()

# 4.5 contains() – Checks if a DataFrame column value contains a a value specified in this function.
#contains
# df.filter(df.fname.contains("Cruise")).show()

#4.6 startswith() & endswith() – Checks if the value of the DataFrame Column starts and ends with a String respectively.
#startswith, endswith()
# df.filter(df.fname.startswith("T")).show()
# df.filter(df.fname.endswith("Cruise")).show()

#4.8 isNull & isNotNull() – Checks if the DataFrame column has NULL or non NULL values.
#isNull & isNotNull
# df.filter(df.lname.isNull()).show()
# df.filter(df.lname.isNotNull()).show()

# 4.9 like() & rlike() – Similar to SQL LIKE expression.
#like , rlike
# df.select(df.fname,df.lname,df.id).filter(df.fname.like("%om")).show()

#4.10 substr() – Returns a Column after getting sub string from the Column
# df.select(df.fname.substr(1,2).alias("substr")).show()

#4.11 when() & otherwise() – It is similar to SQL Case When, executes sequence of expressions until
# it matches the condition and returns a value when match.
# df.select(df.fname,df.lname,when(df.gender=="M","Male"),when(df.gender=="F","Female")
# ,when(df.gender==None,"").otherwise(df.gender).alias("new_gender")).show()

#4.12 isin() – Check if value presents in a List.
#isin
li=["100","200"]
df.select(df.fname,df.lname,df.id) .filter(df.id.isin(li)).show()

# 4.13 getField() – To get the value by key from MapType column and
# by stuct child name from StructType column
data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema1 = StructType([
            StructField('name', StructType([
            StructField('fname', StringType()),
            StructField('lname', StringType())])),
            StructField('languages', ArrayType(StringType())),
            StructField('properties', MapType(StringType(),StringType()))
           ])
df=spark.createDataFrame(data,schema1)
# df.printSchema()

# getField Example
#getField from MapType
# df.select(df.properties.getField("hair")).show()

#getField from Struct
# df.select(df.name.getField("fname")).show()

# 4.14 getItem() – To get the value by index from MapType or
# ArrayTupe & ny key for MapType column.

#getItem() used with ArrayType
# df.select(df.languages.getItem(1)).show()

#getItem() used with MapType
# df.select(df.properties.getItem("hair")).show()
