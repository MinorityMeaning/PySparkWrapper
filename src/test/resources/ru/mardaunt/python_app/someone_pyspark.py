from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()

#Create Schema
schema = StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
])

def start():
    print('Тестовое Pyspark приложение запущено')
    print(spark.sparkContext.applicationId)
    #Create empty DataFrame from empty RDD
    df = spark.createDataFrame(emptyRDD, schema)
    df.select('firstname').distinct().show()