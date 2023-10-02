from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

#Create a SpartSession
spark = SparkSession.builder.appName("MinimumTemperaturePerCapitalSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(city = str(fields[0]),observation_type=str(fields[2]),temperature = float(fields[3])/10)

lines = spark.sparkContext.textFile("1800.csv")

temp = lines.map(mapper)

#Infer the schema, and register the Dataframe as a table
schemaTemperature = spark.createDataFrame(temp).cache()

mintemp = schemaTemperature.filter(schemaTemperature.observation_type == 'TMIN')
mintemp.groupBy("city")\
    .agg(func.round(func.min("temperature"),2).alias("min_temperature")).show()
spark.stop()