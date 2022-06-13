from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("StructType and StructField").getOrCreate()

# PySpark StructType & StructField classes are used to programmatically specify
# the schema to the DataFrame and creating complex columns like nested struct, array and map columns.
# StructType is a collection of StructField’s that defines column name, column data type,
# boolean to specify if the field can be nullable or not and metadata.

# 1. StructType – Defines the structure of the Dataframe
# StructType is a collection or list of StructField objects.
# printSchema() method on the DataFrame shows StructType columns as “struct”.

# 2. StructField – Defines the metadata of the DataFrame column.

# 3. Using PySpark StructType & StructField with DataFrame.
    data = [
        ("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
        ]

    schema = StructType([
        StructField("firstname",StringType()),
        StructField("middlename",StringType()),
        StructField("lastname",StringType()),
        StructField("id", StringType()),
        StructField("gender", StringType()),
        StructField("salary", IntegerType())
        ])

    df = spark.createDataFrame(data=data,schema=schema)
    # df.printSchema()
    # df.show(truncate=False)

# 4. Defining Nested StructType object struct

    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('middlename', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('id', StringType()),
        StructField('gender', StringType()),
        StructField('salary', IntegerType())
    ])

    df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
    # df2.printSchema()
    # df2.show(truncate=False)

# 5. Adding & Changing struct of the DataFrame.
    updatedDF = df2.withColumn("OtherInfo",
                    struct(col("id").alias("identifier"),
                    col("gender").alias("gender"),
                    col("salary").alias("salary"),
                    when(col("salary").cast(IntegerType()) < 2000, "Low")
                    .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                    .otherwise("High").alias("Salary_Grade")
                     )).drop("id", "gender", "salary")

    # updatedDF.printSchema()
    # updatedDF.show(truncate=False)

# 6. Using SQL ArrayType and MapType

    arrayStructureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('middlename', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('hobbies', ArrayType(StringType())),
        StructField('properties', MapType(StringType(), StringType()))
    ])
    # print(arrayStructureSchema)

# 7. Creating StructType object struct from JSON file
    # print(df2.schema.json())

# Alternatively, you could also use df.schema.simpleString(),this will return an relatively simpler schema format.
    # print(df2.schema.simpleString())

# 9. Checking if a Column Exists in a DataFrame
    # print(df.schema.fieldNames().__contains__("firstname"))

