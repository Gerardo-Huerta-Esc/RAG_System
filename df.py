from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Check").getOrCreate()

df = spark.read.parquet("./data/rag_parquet/chunks/")
df.show(5, truncate=False)
