import findspark
findspark.init()
from pyspark.sql import SparkSession
import yaml

spark = SparkSession.builder.appName('theproject')\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()

yaml_file_path = '/home/sushan/airflow/scripts/credentials.yaml'
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

df=spark.read.format("csv").option("header","true").load("/tmp/datas.csv")

jdbc_url = "jdbc:postgresql://localhost:5432/airflow_assignment"

jdbc_properties = {
    "user": config['postgres']["user"],
    "password": str(config['postgres']["password"]),
    "driver": "org.postgresql.Driver"
}

new_df = df.drop("name")  # SomeFiter

new_df.write.parquet("/home/sushan/airflow/airflow_output/comments.parquet", mode="overwrite")
new_df.write.jdbc(url=jdbc_url, table="new_comments_table", mode="overwrite", properties=jdbc_properties)

new_df.show()

spark.stop()
