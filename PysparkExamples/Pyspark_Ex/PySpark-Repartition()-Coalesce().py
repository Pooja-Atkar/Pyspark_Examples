from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark Repartition() vs Coalesce()").master("local[*]").getOrCreate()

# Difference between PySpark repartition() vs coalesce(), repartition() is used to increase or decrease the RDD/DataFrame partitions
# whereas the PySpark coalesce() is used to only decrease the number of partitions in an efficient way.

# One important point to note is,PySpark repartition() and coalesce() are very expensive operations as they shuffle
# the data across many partitions hence try to minimize using these as much as possible.

# 1. PySpark RDD Repartition() vs Coalesce()
# In RDD, you can create parallelism at the time of the creation of an
# RDD using parallelize(), textFile() and wholeTextFiles().

# rdd = spark.sparkContext.parallelize((0,20))
# print("From local[5]"+str(rdd.getNumPartitions()))

rdd1 = spark.sparkContext.parallelize((0,25), 6)
# print("parallelize : "+str(rdd1.getNumPartitions()))
# rdd1.saveAsTextFile("D:\PythonSparkProject\Pyspark_Examples\PysparkExamples\Pyspark_Ex/tmp/partition")

# 1.1 RDD repartition()
# Spark RDD repartition() method is used to increase or decrease the partitions.
# The below example decreases the partitions from 10 to 4 by moving data from all partitions.

# rdd2 = rdd1.repartition(4)
# print("Repartition size : "+str(rdd2.getNumPartitions()))
# rdd2.saveAsTextFile("D:\PythonSparkProject\Pyspark_Examples\PysparkExamples\Pyspark_Ex/tmp/re-partition")

# 1.2 RDD coalesce()
# Spark RDD coalesce() is used only to reduce the number of partitions. This is optimized or improved
# version of repartition() where the movement of the data across the partitions is lower using coalesce.

# rdd3 = rdd1.coalesce(4)
# print("Repartition size : "+str(rdd3.getNumPartitions()))
# rdd3.saveAsTextFile("D:\PythonSparkProject\Pyspark_Examples\PysparkExamples\Pyspark_Ex/tmp/coalesce")

# 2. PySpark DataFrame repartition() vs coalesce()

# Like RDD, you can’t specify the partition/parallelism while creating DataFrame.
# DataFrame by default internally uses the methods specified in Section 1 to determine the default
# partition and splits the data for parallelism.

df=spark.range(0,20)
# print(df.rdd.getNumPartitions())
# df.write.mode("overwrite").csv("D:\PythonSparkProject\Pyspark_Examples\PysparkExamples\Pyspark_Ex/tmp/partition1.csv")

# 2.1 DataFrame repartition()
# Similar to RDD, the PySpark DataFrame repartition() method is used to increase or decrease the partitions.
# The below example increases the partitions from 5 to 6 by moving data from all partitions.

# df2 = df.repartition(6)
# print(df2.rdd.getNumPartitions())

# 2.2 DataFrame coalesce()
# Spark DataFrame coalesce() is used only to decrease the number of partitions.
# This is an optimized or improved version of repartition() where the movement of the data across the partitions is fewer using coalesce.

# df3 = df.coalesce(2)
# print(df3.rdd.getNumPartitions())

# 3. Default Shuffle Partition
# Calling groupBy(), union(), join() and similar functions on DataFrame results in shuffling
# data between multiple executors and even machines and finally repartitions data into 200 partitions by default.
# PySpark default defines shuffling partition to 200 using spark.sql.shuffle.partitions configuration.

# df4 = df.groupBy("id").count()
# print(df4.rdd.getNumPartitions())