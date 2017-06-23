from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("wordscount").setMaster("yarn")
spark_context = SparkContext(conf=config)

rdd = spark_context.textFile("/data/griboedov")
rdd2 = rdd.map(lambda x: x.strip().lower())
rdd3 = rdd2.flatMap(lambda x: x.split(" "))
rdd4 = rdd3.map(lambda x: (x,1))
rdd5 = rdd4.groupByKey()
rdd6 = rdd5.map(lambda (k, v): (k, sum(v)))
words_count = rdd6.collect()

top_words_count = sorted(words_count, key=lambda x: x[1], reverse=True)[:30]
for word, count in top_words_count:
    print word.encode("utf8"), count
