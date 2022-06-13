from pyspark.sql import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("PySpark – create_map()").master("local[*]").getOrCreate()

# PySpark SQL function create_map() is used to convert selected DataFrame columns to MapType,
# create_map() takes a list of columns you wanted to convert as an argument and returns a MapType column.

data = [ ("36636","Finance",3000,"USA"),
    ("40288","Finance",5000,"IND"),
    ("42114","Sales",3900,"USA"),
    ("39192","Marketing",2500,"CAN"),
    ("34534","Sales",6500,"USA") ]
schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('location', StringType(), True)
     ])

df = spark.createDataFrame(data=data,schema=schema)
# df.printSchema()
# df.show(truncate=False)

# Convert DataFrame Columns to MapType
# Now, using create_map() SQL function let’s convert PySpark DataFrame columns salary and location to MapType.

#Convert columns to Map
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),
        lit("location"),col("location")
        )).drop("salary","location")
# df.printSchema()
# df.show(truncate=False)
