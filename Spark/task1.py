from __future__ import print_function
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
import pyspark.sql.functions as fn

spark_session = SparkSession\
                    .builder\
                    .enableHiveSupport()\
                    .appName("Task1")\
                    .master("yarn")\
                    .getOrCreate()

user_logs = spark_session.sparkContext.textFile("/data/access_logs/big_log/")

parsed_logs = user_logs.map(lambda x: [x.split(" ")[0]])

schema = tp.StructType().add("ip", tp.StringType())\

user_log_df = spark_session.createDataFrame(parsed_logs, schema)

has_seven = fn.udf(lambda line : '7' in line, tp.BooleanType())

top_5 = user_log_df.filter(has_seven(user_log_df.ip))\
                   .groupby("ip")\
                   .count()\
                   .orderBy(fn.desc("count"))\
                   .take(5)
for ip, count in top_5:
    print(ip, count, sep='\t')
