from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrame Examples").master("local[*]").getOrCreate()

# You can manually create a PySpark DataFrame using toDF() and createDataFrame() methods.
# You can also create PySpark DataFrame from data sources like TXT, CSV, JSON, ORV, Avro, Parquet, XML
# formats by reading from HDFS, S3, DBFS, Azure Blob file systems e.t.c.

# In order to create a DataFrame from a list we need the data hence, first,
# let’s create the data and the columns that are needed.

columns = ["language","user_count"]
data = [("java","20000"),("python","100000"),("scala","3000")]

# 1. Create DataFrame from RDD

rdd =spark.sparkContext.parallelize(data)
# 1.1 Using toDF() function
# dfFromRdd1 = rdd.toDF()
# dfFromRdd1.printSchema()

# If you wanted to provide column names to the DataFrame use toDF() method.
# dfFromRdd1 = rdd.toDF(columns)
# dfFromRdd1.printSchema()

# 1.2 Using createDataFrame() from SparkSession
# dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
# dfFromRDD2.show()

# 2.1 Using createDataFrame() from SparkSession
# dfFromData2 = spark.createDataFrame(data).toDF(*columns)
# dfFromData2.show()

# 2.3 Create DataFrame with schema
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# 3. Create DataFrame from Data sources
# 3.1 Creating DataFrame from CSV
# 3.2. Creating from text (TXT) file
# 3.3. Creating from JSON file
# 3.4. Creating from orc file
# 3.5. Creating from parquet file



