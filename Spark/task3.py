from __future__ import print_function
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
import pyspark.sql.functions as fn

spark_session = SparkSession\
                    .builder\
                    .enableHiveSupport()\
                    .appName("Task3")\
                    .master("yarn")\
                    .getOrCreate()

spark_session.sparkContext.addFile('parse_tool.py')

from parse_tool import parse_logs

# User logs collection
user_logs = spark_session.sparkContext.textFile("/data/access_logs/big_log/")

parsed_logs = user_logs.map(parse_logs) \
                       .filter(lambda parse_res : parse_res[8] != "") \
                       .map(lambda parse_res : [
                          parse_res[0],                       # ip
                          parse_res[0] + '_' + parse_res[7],  # ip+user_agent
                          parse_res[8]                        # hour
                       ])

schema = tp.StructType().add("ip", tp.StringType()) \
                        .add("user_id", tp.StringType()) \
                        .add("hour", tp.ShortType())

user_log_df = spark_session.createDataFrame(parsed_logs, schema)

res = user_log_df.groupby("hour") \
                 .agg(fn.countDistinct("user_id").alias("unique_visitors_count")) \
                 .orderBy("hour") \
                 .collect()

for hour, count in res:
    print(hour, count, sep='\t')