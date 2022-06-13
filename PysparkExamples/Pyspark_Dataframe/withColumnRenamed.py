from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("withColumnRenamed Example").master("local[*]").getOrCreate()

#Use PySpark withColumnRenamed() to rename a DataFrame column,
# we often need to rename one column or multiple (or all) columns on PySpark DataFrame.

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

schema3 = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType()),
             StructField('middlename', StringType()),
             StructField('lastname', StringType())
             ])),
         StructField('dob', StringType()),
         StructField('gender', StringType()),
         StructField('salary', IntegerType())
         ])
df = spark.createDataFrame(data = dataDF, schema = schema3)
df.printSchema()
# df.show()

# 1. PySpark withColumnRenamed – To rename DataFrame column name
# PySpark withColumnRenamed() Syntax:
# withColumnRenamed(existingName, newNam)
# df.withColumnRenamed("dob","DateOfBirth").printSchema()

# 2. PySpark withColumnRenamed – To rename multiple columns
# df2 = df.withColumnRenamed("dob","DateOfBirth").withColumnRenamed("salary","salary_amount")
# df2.printSchema()

# 3. Using PySpark StructType – To rename a nested column in
# This statement renames firstname to fname and lastname to lname within name structure.
schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])

# df.select(col("name").cast(schema2),col("dob"), col("gender"),col("salary")).printSchema()

# 4. Using Select – To rename nested elements.
# Let’s see another way to change nested columns by transposing the structure to flat.

# df.select(col("name.firstname").alias("fname"),
#   col("name.middlename").alias("mname"),
#   col("name.lastname").alias("lname"),
#   col("dob"),col("gender"),col("salary")).printSchema()

# 5. Using PySpark DataFrame withColumn – To rename nested columns

from pyspark.sql.functions import *
df4 = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")).drop("name")
# df4.printSchema()

# 7. Using toDF() – To change all columns in a PySpark DataFrame
# newColumns = ["newCol1","newCol2","newCol3","newCol4"]
# df.toDF(*newColumns).printSchema()
