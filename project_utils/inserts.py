import findspark
findspark.init()
from pyspark.sql import Row, SparkSession


def insert_keywords(sc, text, spark, time):
    if text:
        keywords_rdd = sc.parallelize(text)
        keywords_rdd = keywords_rdd.map(lambda x: x.lower())
        if keywords_rdd.count() > 0:
            keywords_data_frame = spark.createDataFrame(keywords_rdd.map(lambda x: Row(sentence=x, timestamp=time)))
            keywords_data_frame.createOrReplaceTempView("tweets")
            spark.sql("create database if not exists p3")
            spark.sql("use p3")
            keywords_data_frame.write.mode("append").saveAsTable("tweets")
            print("Inserted tweets into table")
    else:
        print("No tweets to insert")