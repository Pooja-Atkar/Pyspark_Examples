from pyspark.sql import *
spark = SparkSession.builder.appName("PySpark parallelize()").master("local[*]").getOrCreate()

# PySpark parallelize() is a function in SparkContext and is used to create an RDD from a list collection.

rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10])
# print(rdd.collect())


rdd1=spark.sparkContext.parallelize([1,2,3,4,5])
rddCollect = rdd1.collect()
print("Number of Partitions: "+str(rdd1.getNumPartitions()))
print("Action: First element: "+str(rdd1.first()))
# print(rddCollect)

# create empty RDD by using sparkContext.parallelize

emptyRDD = spark.sparkContext.emptyRDD()
emptyRDD2 = rdd=spark.sparkContext.parallelize([])

# print("is Empty RDD : "+str(emptyRDD2.isEmpty()))
