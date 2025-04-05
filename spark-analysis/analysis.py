import os

os.environ['SPARK_HOME'] = "/opt/spark"  
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, to_timestamp, expr

spark = SparkSession.builder.appName("KMeansAnalysis").getOrCreate()
df = spark.read.option("multiline", "true").json("hdfs://hadoop-namenode:9000/data/pollution.json")

df= df.drop("coordinates", "summary")

df = df.withColumn("datetimeFrom", to_timestamp(col("period.datetimeFrom.utc")))
df = df.withColumn("datetimeTo", to_timestamp(col("period.datetimeTo.utc")))

df = df.withColumn("duration_seconds", expr("timestampdiff(SECOND, datetimeFrom, datetimeTo)"))

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
