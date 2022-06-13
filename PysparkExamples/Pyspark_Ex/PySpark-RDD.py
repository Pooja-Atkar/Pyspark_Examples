from pyspark.sql import *

spark = SparkSession.builder.appName("PySpark RDD").master("local[*]").getOrCreate()

#Create RDD from parallelize
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)
print(rdd)

# Create RDD using sparkContext.textFile()

#Create RDD from external Data source
# rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

# Create RDD using sparkContext.wholeTextFiles()

#Reads entire file into a RDD as single record.
# rdd3 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")

# Create empty RDD using sparkContext.emptyRDD

# Creates empty RDD with no partition
rdd = spark.sparkContext.emptyRDD
# rddString = spark.sparkContext.emptyRDD[String]

# Creating empty RDD with partition

#Create empty RDD with partition
rdd2 = spark.sparkContext.parallelize([],10) #This creates 10 partitions

# RDD Parallelize
# getNumPartitions() – This a RDD function which returns a number of partitions our dataset split into.

print("initial partition count:"+str(rdd.getNumPartitions()))
#Outputs: initial partition count:2

# Set parallelize manually – We can also set a number of partitions manually, all, we need is, to pass a number of partitions as the second parameter to these functions for example
# sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10).

# Repartition and Coalesce
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)
reparRdd = rdd.repartition(4)
print("re-partition count:"+str(reparRdd.getNumPartitions()))
#Outputs: "re-partition count:4

# PySpark RDD Operations
# RDD transformations – Transformations are lazy operations, instead of updating an RDD,
# these operations return another RDD.
# RDD actions – operations that trigger computation and return RDD values.

# RDD Transformations with example
# Transformations on PySpark RDD returns another RDD and transformations are lazy meaning
# they don’t execute until you call an action on RDD.
# Some transformations on RDD’s are flatMap(), map(), reduceByKey(), filter(), sortByKey() and
# return new RDD instead of updating the current.


rdd = spark.sparkContext.textFile("/tmp/test.txt")

# flatMap – flatMap() transformation flattens the RDD after applying the function and returns a new RDD.
# On the below example, first, it splits each record by space in an RDD and finally flattens it.
# Resulting RDD consists of a single word on each record.

# rdd2 = rdd.flatMap(lambda x: x.split(" "))

# map – map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c,
# the output of map transformations would always have the same number of records as input.

rdd3 = rdd2.map(lambda x: (x,1))

# reduceByKey – reduceByKey() merges the values for each key with the function specified. In our example,
# it reduces the word string by applying the sum function on value.
# The result of our RDD contains unique words and their count.

rdd4 = rdd3.reduceByKey(lambda a,b: a+b)

# sortByKey – sortByKey() transformation is used to sort RDD elements on key.
# In our example, first, we convert RDD[(String,Int]) to RDD[(Int, String])
# using map transformation and apply sortByKey which ideally does sort on an integer value.
# And finally, foreach with println statements returns all words in RDD and their count as key-value pair.

rdd6 = rdd3.map(lambda x: (x[1],x[0])).sortByKey()
# #Print rdd6 result to console
# print(rdd6.collect())

# filter – filter() transformation is used to filter the records in an RDD.
# In our example we are filtering all words starts with “a”.

rdd4 = rdd3.filter(lambda x : 'an' in x[1])
# print(rdd4.collect())

# RDD Actions with example
# count() – Returns the number of records in an RDD
# Action - count
# print("Count : "+str(rdd5.count()))

# first() – Returns the first record.
# # Action - first
# firstRec = rdd6.first()
# print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])

# max() – Returns max record.
# # Action - max
# datMax = rdd6.max()
# print("Max Record : "+str(datMax[0]) + ","+ datMax[1])

# reduce() – Reduces the records to single, we can use this to count or sum.
# # Action - reduce
# totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
# print("dataReduce Record : "+str(totalWordCount[0]))

# take() – Returns the record specified as an argument.
# # Action - take
# data3 = rdd6.take(3)
# for f in data3:
#     print("data3 Key:"+ str(f[0]) +", Value:"+f[1])

# collect() – Returns all data from RDD as an array.
# Be careful when you use this action when you are working with huge RDD with millions and
# billions of data as you may run out of memory on the driver.
# # Action - collect
# data = rdd6.collect()
# for f in data:
#     print("Key:"+ str(f[0]) +", Value:"+f[1])

# saveAsTextFile() – Using saveAsTestFile action, we can write the RDD to a text file.
# rdd6.saveAsTextFile("/tmp/wordCount")