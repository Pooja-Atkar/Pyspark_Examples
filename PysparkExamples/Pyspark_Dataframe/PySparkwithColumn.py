from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("with Column Example").master("local[*]").getOrCreate()

# PySpark withColumn() is a transformation function of DataFrame which is used to change the value,
# convert the datatype of an existing column, create a new column, and many more.


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

# 1. Change DataType using PySpark withColumn()
# df.withColumn("salary",col("salary").cast("Integer")).show()

# 2. Update The Value of an Existing Column
# df.withColumn("salary",col("salary")*100).show()

# 3. Create a Column from an Existing
# df.withColumn("CopiedColumn",col("salary")* -1).show()

# 4. Add a New Column using withColumn()
# df.withColumn("Country", lit("USA")).show()
# df.withColumn("Country", lit("USA")) .withColumn("anotherColumn",lit("anotherValue")) .show()

# 5. Rename Column Name
# df.withColumnRenamed("gender","sex").show(truncate=False)

# 6. Drop Column From PySpark DataFrame
# df.drop("salary").show()
