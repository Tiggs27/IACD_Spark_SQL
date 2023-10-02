from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

#Create a SpartSession
spark = SparkSession.builder.appName("TotalSpentSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(costumerID = int(fields[0]),amount = float(fields[2]))

lines = spark.sparkContext.textFile("customer-orders.csv")
costumer = lines.map(mapper)

#Infer the schema, and register the Dataframe as a table
schemaAmount = spark.createDataFrame(costumer).cache()

schemaAmount.groupBy("costumerID")\
    .agg(func.round(func.sum("amount"),2).alias("amount_sum")).show()
spark.stop()