from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Joins Example").getOrCreate()

# PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames;
# it supports all basic join type operations available in traditional SQL
# like INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN.

# 1. PySpark Join Syntax
# join(self, other, on=None, how=None)

# join() operation takes parameters as below and returns DataFrame.
# param other: Right side of the join
# param on: a string for the join column name
# param how: default inner.
# Must be one of inner, cross, outer,full, full_outer, left, left_outer, right, right_outer,left_semi, and left_anti.

# 2. PySpark Join Types

emp = [(1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)]

empColumns = ["emp_id","name","superior_emp_id","year_joined", "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
# empDF.printSchema()
# empDF.show()

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show()

# 3. PySpark Inner Join DataFrame
# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)

# 4. PySpark Full Outer Join

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer").show()
# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show()
# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show()

# 5. PySpark Left Outer Join

# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id,"left").show()
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter").show()

# 6. Right Outer Join

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right").show()
# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter").show()

# 7. Left Semi Join

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show()

# 8. Left Anti Join

# empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti").show()

# 9. PySpark Self Join

# empDF.alias("emp1").join(empDF.alias("emp2"), \
#     col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
#     .select(col("emp1.emp_id"),col("emp1.name"), \
#     col("emp2.emp_id").alias("superior_emp_id"), \
#     col("emp2.name").alias("superior_emp_name")).show()

# 4. Using SQL Expression

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

# joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show()

# joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show()

# 5. PySpark SQL Join on multiple DataFrames
# df1.join(df2,df1.id1 == df2.id2,"inner").join(df3,df1.id1 == df3.id3,"inner")
