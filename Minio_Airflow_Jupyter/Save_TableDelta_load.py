## 1.4.1 Guardar la tabla consolidada, en una Tabla Delta

from pyspark.sql import SparkSession
from delta import *
import pandas as pd

#!pip install delta-spark
S3_ACCESS_KEY = "nIfbWDx9lhnt5Y2I58bh"
S3_BUCKET = "hudbayprocessed2"
S3_SECRET_KEY = "OjbcZm24KUk8jlEPqedS7FzmvHilnwkN5mK77E7e"
S3_ENDPOINT = "192.168.25.223"
#S3_ENDPOINT = "minio.minio-user.svc.cluster.local:80"

#hadoop_common_jar = "/home/jovyan/jars/hadoop-client-api-3.3.4.jar,/home/jovyan/jars/hadoop-client-runtime-3.3.4.jar,/home/jovyan/jars/hadoop-common-3.3.4.jar"
spark = SparkSession \
  .builder \
  .appName("AppDeltaMinio9") \
  .config("spark.executor.memory", "8g") \
  .config("spark.executor.cores", "2") \
  .config("spark.driver.memory", "4g") \
  .config("spark.driver.cores", "2") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,com.amazonaws:aws-java-sdk-pom:1.12.720") \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
  .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
  .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
  .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
  .getOrCreate()
  #.config("spark.databricks.delta.schema.columnMapping.mode", "name") \
  #.config("spark.jars", hadoop_common_jar) \
  
# Opcional
# Reemplaza caracteres problemáticos en los nombres de las columnas
# resultado.columns = [col.replace(" ", "_").replace("(", "").replace(")", "").replace("\n", "").replace("=", "") for col in resultado.columns]

# Supongamos que tienes un DataFrame de pandas llamado 'datos'
spark_df = spark.createDataFrame(resultado)
#spark_df2 = spark.createDataFrame(datos).repartition(30)

spark_df.write.format("delta").save("s3a://hudbayprocessed2/tabladelta_datacamion_ciclos")
