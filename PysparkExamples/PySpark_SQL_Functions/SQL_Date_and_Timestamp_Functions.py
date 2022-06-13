from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Date and Time Functions").master("local[*]").getOrCreate()

# PySpark Date and Timestamp Functions are supported on DataFrame and SQL queries and they work similarly
# to traditional SQL, Date and Time are very important if you are using PySpark for ETL. Most of all these
# functions accept input as, Date type, Timestamp type, or String. If a String used, it should be in a default
# format that can be cast to date.

# DateType default format is yyyy-MM-dd
# TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS
# Returns null if the input is a string that can not be cast to Date or Timestamp.
# PySpark SQL provides several Date & Timestamp functions hence keep an eye on and understand these.
# Always you should choose these functions instead of writing your own functions (UDF) as these functions
# are compile-time safe, handles null, and perform better when compared to PySpark UDF.
# If your PySpark application is critical on performance try to avoid using custom UDF at all costs as these
# are not guarantee performance.

# The default format of the PySpark Date is yyyy-MM-dd.

# PySpark SQL Date and Timestamp Functions Examples
# Following are the most used PySpark SQL Date and Timestamp Functions with examples,
# you can use these on DataFrame and SQL expressions.

# Create SparkSession

data = [["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df = spark.createDataFrame(data,["id","input"])
# df.show()

# current_date()
# Use current_date() to get the current system date. By default, the data will be returned in yyyy-dd-mm format.
# df.select(current_date().alias("current_date")).show(1)

# date_format()
# The below example uses date_format() to parses the date and converts from yyyy-dd-mm to MM-dd-yyyy format.
# df.select(col("input"),date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()

# to_date()
# Below example converts string in date format yyyy-MM-dd to a DateType yyyy-MM-dd using to_date().
# You can also use this to convert into any specific format. PySpark supports all patterns supports on Java
# DateTimeFormatter.
# df.select(col("input"),to_date(col("input"), "yyy-MM-dd").alias("to_date")).show()

# datediff()
# The below example returns the difference between two dates using datediff().
# df.select(col("input"),datediff(current_date(),col("input")).alias("datediff")).show()

# months_between()
# The below example returns the months between two dates using months_between().
# df.select(col("input"),months_between(current_date(),col("input")).alias("months_between")).show()

# trunc()
# The below example truncates the date at a specified unit using trunc().
# df.select(col("input"),
#     trunc(col("input"),"Month").alias("Month_Trunc"),
#     trunc(col("input"),"Year").alias("Month_Year"),
#     trunc(col("input"),"Month").alias("Month_Trunc")).show()

# add_months() , date_add(), date_sub()
# Here we are adding and subtracting date and month from a given input.

# df.select(col("input"),
#     add_months(col("input"),3).alias("add_months"),
#     add_months(col("input"),-3).alias("sub_months"),
#     date_add(col("input"),4).alias("date_add"),
#     date_sub(col("input"),4).alias("date_sub")).show()

# year(), month(), month(),next_day(), weekofyear()

# df.select(col("input"),
#      year(col("input")).alias("year"),
#      month(col("input")).alias("month"),
#      next_day(col("input"),"Sunday").alias("next_day"),
#      weekofyear(col("input")).alias("weekofyear")).show()

# dayofweek(), dayofmonth(), dayofyear()

# df.select(col("input"),
#      dayofweek(col("input")).alias("dayofweek"),
#      dayofmonth(col("input")).alias("dayofmonth"),
#      dayofyear(col("input")).alias("dayofyear")).show()

# current_timestamp()
# Timestamp Functions that you can use on SQL and on DataFrame.

data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df2=spark.createDataFrame(data,["id","input"])
# df2.show()

# returns the current timestamp in spark default format yyyy-MM-dd HH:mm:ss
#current_timestamp()
# df2.select(current_timestamp().alias("current_timestamp")).show(1,truncate=False)

# to_timestamp()
# Converts string timestamp to Timestamp type format.
# df2.select(col("input"),to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show()

# hour(), Minute() and second()

# data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
# df3=spark.createDataFrame(data,["id","input"])

# df3.select(col("input"),
#     hour(col("input")).alias("hour"),
#     minute(col("input")).alias("minute"),
#     second(col("input")).alias("second")).show()
