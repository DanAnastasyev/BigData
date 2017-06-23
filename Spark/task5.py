from __future__ import print_function
import re

from pyspark.sql import SparkSession
import pyspark.sql.types as tp
import pyspark.sql.functions as fn

spark_session = SparkSession\
                    .builder\
                    .enableHiveSupport()\
                    .appName("Task5")\
                    .master("local[3]")\
                    .config("spark.sql.crossJoin.enabled", "true")\
                    .getOrCreate()

social_graph = spark_session.sparkContext.textFile("/data/social_graph_sample/", minPartitions=40)

social_graph_pattern = re.compile('(\d+),')
def parse_social_graph(line):
    user_id, friends = line.strip().split('\t')
    user_id = int(user_id)
    friends = [int(x) for x in social_graph_pattern.findall(friends)]
    return (
        user_id,
        friends,
    )

parsed_graph = social_graph.map(parse_social_graph) \
                           .map(lambda parse_res : [
                              parse_res[0],
                              parse_res[1]
                           ]) \
                           .flatMap(lambda pair : [(pair[0], friend_id) for friend_id in pair[1]]) \
                           .flatMap(lambda pair : [(pair[0], pair[1]), (pair[1], pair[0])]) \
                           .distinct()

schema = tp.StructType().add("id", tp.IntegerType()) \
                        .add("friend_id", tp.IntegerType())

social_graph_df = spark_session.createDataFrame(parsed_graph, schema)

social_graph_df.show()

res = social_graph_df.limit(500) \
                     .groupBy("id") \
                     .count() \
                     .orderBy(fn.desc("count")) \
                     .limit(100)

intersection_udf = fn.udf(lambda arr1, arr2 : len(set(arr1) & set(arr2)), tp.IntegerType())

res2 = res.join(social_graph_df, res.id == social_graph_df.id) \
   .select(res.influencer_id, social_graph_df.friends) \
   .alias('top_influencers') \
   .join(social_graph_df.alias('friends_info').limit(100)) \
   .select('top_influencers.influencer_id', \
            'friends_info.id', \
            intersection_udf('top_influencers.friends', 'friends_info.friends').alias('friends_count')) \
   .alias('counts')

# res3 = res2.groupBy('influencer_id') \
#    .agg(fn.max("friends_count").alias("max")) \
#    .alias("maxs")

# res3.join(res2) \
#     .show()