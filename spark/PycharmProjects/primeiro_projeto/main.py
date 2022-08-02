from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder.appName('Projeto Python').getOrCreate()

juros = spark.read.json('/user/nathalia/data/input/juros_selic/juros_selic.json')
juros.collect()
juros.write.parquet('/user/nathalia/projeto_python/', mode='overwrite')

sleep(100)

spark.stop()