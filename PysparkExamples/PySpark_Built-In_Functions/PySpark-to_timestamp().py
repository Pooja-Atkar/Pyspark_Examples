from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark to_timestamp()").master("local[*]").getOrCreate()

# Use <em>to_timestamp</em>() function to convert String to Timestamp (TimestampType) in PySpark.
# The converted time would be in a default format of MM-dd-yyyy HH:mm:ss.SSS.

# Syntax – to_timestamp()

# Syntax: to_timestamp(timestampString:Column)
# Syntax: to_timestamp(timestampString:Column,format:String)

# This function has above two signatures that defined in PySpark SQL Date & Timestamp Functions,
# the first syntax takes just one argument and the argument should be in Timestamp format ‘MM-dd-yyyy HH:mm:ss.SSS‘,
# when the format is not in this format, it returns null.

# The second signature takes an additional String argument to specify the format of the input Timestamp;
# this support formats specified in SimeDateFormat.
# Using this additional argument, you can cast String from any format to Timestamp type in PySpark.

# Convert String to PySpark Timestamp type
# we convert the string pattern which is in PySpark default format to Timestamp type,
# since the input DataFrame column is in default Timestamp format, we use the first signature for conversion.
# And the second example uses the cast function to do the same.

from pyspark.sql.functions import *

df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
# df.withColumn("timestamp",to_timestamp("input_timestamp")) \
#   .show(truncate=False)

# Custom string format to Timestamp type
#when dates are not in Spark TimestampType format 'yyyy-MM-dd  HH:mm:ss.SSS'.
#Note that when dates are not in Spark Tiemstamp format, all Spark functions returns null
#Hence, first convert the input dates to Spark DateType using to_timestamp function

# df.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')) \
#   .show()

# SQL Example

#SQL string to TimestampType
# spark.sql("select to_timestamp('2019-06-24 12:01:19.000') as timestamp")

#SQL CAST timestamp string to TimestampType
# spark.sql("select timestamp('2019-06-24 12:01:19.000') as timestamp")

#SQL Custom string to TimestampType
# spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as timestamp")
