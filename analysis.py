# Importer les bibliothèques nécessaires pour Spark et l'analyse k-means
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, unix_timestamp, to_timestamp, expr
from hdfs import InsecureClient
import json 

spark = SparkSession.builder.appName("KMeansAnalysis").getOrCreate()
# client = InsecureClient('http://localhost:9870', user='root')

# with client.read('/data/pollution.json', encoding='utf-8') as reader:
#     json_data = json.load(reader)

# df = spark.read.json(spark.sparkContext.parallelize([json.dumps(json_data)]))
# df = spark.read.option("multiline", "true").json("hdfs://localhost:9870/data/pollution.json")
# df.printSchema()
# df.show(truncate=False)

# df = spark.read.option("multiline", "true").json(json_data)
# df.printSchema()
# df.show(truncate=False)

df = spark.read.option("multiline", "true").json("D:\\big-data-projects\\pollution.json")

df= df.drop("coordinates", "summary")

df.select("period.datetimeFrom.utc", "period.datetimeTo.utc").show(truncate=False)

df = df.withColumn("datetimeFrom", to_timestamp(col("period.datetimeFrom.utc")))
df = df.withColumn("datetimeTo", to_timestamp(col("period.datetimeTo.utc")))

df = df.withColumn("duration_seconds", expr("timestampdiff(SECOND, datetimeFrom, datetimeTo)"))
# df.select("datetimeFrom", "datetimeTo", "duration_seconds").show()

df_selected = df.select(
    col("value"),
    col("parameter.id").alias("parameter_id"),
    col("coverage.percentComplete").alias("percentComplete"),
    col("coverage.percentCoverage").alias("percentCoverage"),
    col("duration_seconds")
)

assembler = VectorAssembler(
    inputCols=["value", "parameter_id", "percentComplete", "percentCoverage", "duration_seconds"],
    outputCol="features"
)
df_features = assembler.transform(df_selected).select("features")

kmeans = KMeans(k=3, seed=1, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(df_features)
df_clusters = model.transform(df_features)

df_clusters.show()
