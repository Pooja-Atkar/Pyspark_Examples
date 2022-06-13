from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark date_format()").master("local[*]").getOrCreate()

# In PySpark use date_format() function to convert the DataFrame column from Date to String format.
# date_format() – function formats Date to String format.

# Following are Syntax and Example of date_format() Function:

# Syntax:  date_format(column,format)
# Example: date_format(current_timestamp(),"yyyy MM dd").alias("date_format")

# The current system date from current_date() and timestamp from the current_timestamp()
# function and converts it to String format on DataFrame.


from pyspark.sql.functions import *

# df=spark.createDataFrame([["1"]],["id"])
# df.select(current_date().alias("current_date"), \
#       date_format(current_timestamp(),"yyyy MM dd").alias("yyyy MM dd"), \
#       date_format(current_timestamp(),"MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"), \
#       date_format(current_timestamp(),"yyyy MMM dd").alias("yyyy MMMM dd"), \
#       date_format(current_timestamp(),"yyyy MMMM dd E").alias("yyyy MMMM dd E") \
#    ).show()

# Alternatively, you can convert Data to String with SQL by using same functions.

# SQL
# spark.sql("select current_date() as current_date, "+
#       "date_format(current_timestamp(),'yyyy MM dd') as yyyy_MM_dd, "+
#       "date_format(current_timestamp(),'MM/dd/yyyy hh:mm') as MM_dd_yyyy, "+
#       "date_format(current_timestamp(),'yyyy MMM dd') as yyyy_MMMM_dd, "+
#       "date_format(current_timestamp(),'yyyy MMMM dd E') as yyyy_MMMM_dd_E").show()


