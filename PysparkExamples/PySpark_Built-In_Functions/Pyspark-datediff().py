from pyspark.sql import *

spark = SparkSession.builder.appName("datediff() function").master("local[*]").getOrCreate()

# PySpark – Difference between two dates (days, months, years)

# Using PySpark SQL functions datediff(), months_between() you can calculate
# the difference between two dates in days, months, and year, let’s see this by using a DataFrame example.
# You can also use these to calculate age.

# datediff() Function
# First Let’s see getting the difference between two dates using datediff() PySpark function.

from pyspark.sql.functions import *
data = [("1","2019-07-01"),("2","2019-06-24"),("3","2019-08-24")]
df=spark.createDataFrame(data=data,schema=["id","date"])

# df.select(
#       col("date"),
#       current_date().alias("current_date"),
#       datediff(current_date(),col("date")).alias("datediff")
#     ).show()

# months_between() Function
# Now, Let’s see how to get month and year differences between two dates using months_between() function.

from pyspark.sql.functions import *
df.withColumn("datesDiff", datediff(current_date(),col("date"))) \
  .withColumn("montsDiff", months_between(current_date(),col("date"))) \
  .withColumn("montsDiff_round",round(months_between(current_date(),col("date")),2)) \
  .withColumn("yearsDiff",months_between(current_date(),col("date"))/lit(12)) \
  .withColumn("yearsDiff_round",round(months_between(current_date(),col("date"))/lit(12),2)) \
  .show()

# Note that here we use round() function and lit() functions on top of months_between() to get the year between two dates.

# the difference between two dates when dates are not in PySpark DateType format yyyy-MM-dd.
# when dates are not in DateType format, all date functions return null.
# Hence, you need to first convert the input date to Spark DateType using to_date() function.

from pyspark.sql.functions import *
data2 = [("1","07-01-2019"),("2","06-24-2019"),("3","08-24-2019")]
df2=spark.createDataFrame(data=data2,schema=["id","date"])
# df2.select(
#     to_date(col("date"),"MM-dd-yyyy").alias("date"),
#     current_date().alias("endDate")).show()

# SQL Example

# spark.sql("select round(months_between('2019-07-01',current_date())/12,2) as years_diff").show()
