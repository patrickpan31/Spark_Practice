from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName('practice') \
    .getOrCreate()

df = spark.read.csv('art_spark.csv')
df.printSchema()
df.show()

df = spark.read.csv('art_spark.csv', header= True)
df.printSchema()
df.show()

df = spark.read.option('header','true').csv('art_spark.csv', inferSchema= True)
df.printSchema()
df.show()

schema = StructType([
    StructField("GENERIC", StringType(), True),
    StructField("TRADE", StringType(), True),
    StructField("TYPE", StringType(), True),
    StructField("NUMBER", IntegerType(), True)
])

df = spark.read.csv('art_spark.csv', schema = schema)
df.printSchema()
df.show()

df = spark.read.csv('art_spark.csv', header= True, schema = schema)
df.printSchema()
df.show()

print(df.columns)

df.select('TYPE').show()
df.select(['TYPE','NUMBER']).show()
df.describe("number").show()

#add and delete column
add_two = udf(lambda x: x+1, IntegerType())
df = df.withColumn('NEWNUM', add_two('NUMBER'))
df.drop('NEWNUM').show()

#rename the column
df.withColumnRenamed('NUMBER', 'NUMBERCOUNT').show()


