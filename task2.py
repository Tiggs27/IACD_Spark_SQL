from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row

conf = SparkConf().setMaster("local").setAppName("Book")
sc = SparkContext(conf=conf)

#Create a SpartSession
spark = SparkSession.builder.appName("BookSQL").getOrCreate()

lines = sc.textFile("Book")

lines = lines.flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x,0))

data = []
for i in lines.collect():
    data.append(i)


df_schema= spark.createDataFrame(data).toDF("word","aux_value")

df_schema.printSchema()

count_word = df_schema.groupBy("word")\
    .agg(func.round(func.count("word"),2).alias("count_words"))

count_word.show()
spark.stop()