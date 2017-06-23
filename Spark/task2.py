from __future__ import print_function
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
import pyspark.sql.functions as fn
import re
from datetime import datetime as dt

spark_session = SparkSession\
                    .builder\
                    .enableHiveSupport()\
                    .appName("Task2")\
                    .master("yarn")\
                    .getOrCreate()

spark_session.sparkContext.addFile('parse_tool.py')

from parse_tool import parse_logs, parse_geoinfo

# User logs collection
user_logs = spark_session.sparkContext.textFile("/data/access_logs/big_log/")

parsed_logs = user_logs.map(parse_logs)\
                       .map(lambda parse_res : [
                         parse_res[0],
                         parse_res[0] + parse_res[7]
                       ])

schema = tp.StructType().add("ip", tp.StringType())\
                        .add("user_id", tp.StringType())

user_log_df = spark_session.createDataFrame(parsed_logs, schema)


# Geo info collection
geoip = spark_session.sparkContext.textFile("/data/access_logs/geoiplookup/")

parsed_geoip = geoip.map(parse_geoinfo)\
                    .map(lambda parse_res : [
                         parse_res[0],
                         parse_res[1]
                       ])

schema = tp.StructType().add("ip", tp.StringType())\
                        .add("location", tp.StringType())

geoip_df = spark_session.createDataFrame(parsed_geoip, schema)


# Request
top_5 = user_log_df.join(geoip_df, user_log_df.ip == geoip_df.ip)\
                   .groupby(geoip_df.location)\
                   .agg(fn.countDistinct(user_log_df.user_id).alias("unique_visitors_count"))\
                   .orderBy(fn.desc("unique_visitors_count"))\
                   .take(5)

for location, count in top_5:
    print(location, count, sep='\t')