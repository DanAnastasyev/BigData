from __future__ import print_function
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
import pyspark.sql.functions as fn

spark_session = SparkSession\
                    .builder\
                    .enableHiveSupport()\
                    .appName("Task4")\
                    .master("yarn")\
                    .getOrCreate()

spark_session.sparkContext.addFile('parse_tool.py')

from parse_tool import parse_logs

# User logs collection
user_logs = spark_session.sparkContext.textFile("/data/access_logs/big_log/")

parsed_logs = user_logs.map(parse_logs) \
                       .map(lambda parse_res : [
                          parse_res[0] + '_' + parse_res[7],
                          parse_res[3]
                       ])

schema = tp.StructType().add("user_id", tp.StringType())\
                        .add("request_id", tp.StringType())

user_log_df = spark_session.createDataFrame(parsed_logs, schema)

user_log_df_1 = user_log_df.alias("df_1")
user_log_df_2 = user_log_df.alias("df_2")

is_request_to_id = fn.udf(lambda line : line.startswith('/id'), tp.BooleanType())

top_5 = user_log_df_1.groupBy(user_log_df.user_id) \
                     .count() \
                     .orderBy(fn.desc("count")) \
                     .limit(100) \
                     .join(user_log_df_2, user_log_df_1.user_id == user_log_df_2.user_id) \
                     .filter(is_request_to_id("request_id")) \
                     .groupBy("request_id") \
                     .count() \
                     .orderBy(fn.desc("count")) \
                     .take(5)

for request_id, count in top_5:
    print(request_id, count, sep='\t')