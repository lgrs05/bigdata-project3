import findspark

from project_utils.inserts import insert_keywords
from project_utils.writes import write_keywords

findspark.init()
import re, csv
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import rand
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime, timedelta
try:
    import json
except ImportError:
    import simplejson as json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars  /home/luis.rivera157/spark/spark-2.1.1-bin-hadoop2.7/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config("spark.sql.warehouse.dir", 'hdfs://master:9000/user/hive/warehouse').enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def parse_text(text):
    text = text.lower()
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', text)
    text = re.sub('@[^\s]+', 'AT_USER', text)
    text = re.sub('[\s]+', ' ', text)
    text = re.sub(r'#([^\s]+)', r'\1', text)
    text = text.strip('\'"')
    return text


def start_consumer():
    context = StreamingContext(sc, 300)
    dStream = KafkaUtils.createDirectStream(context, ["trump"], {"metadata.broker.list": "136.145.216.152:9092,136.145.216.163:9092,136.145.216.168:9092,136.145.216.173:9092,136.145.216.175:9092"})
    dStream.foreachRDD(run)
    context.start()
    context.awaitTermination()


def run(time, rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    tweets = rdd.collect()
    spark = getSparkSessionInstance(rdd.context.getConf())
    text = [tweet["text"] for tweet in tweets if "text" in tweet]
    insert_keywords(sc, text, spark, time)
    global last_refresh
    if datetime.now() > last_refresh + timedelta(hours=1):
        write_keywords(spark)
        last_refresh = datetime.now()


if __name__ == "__main__":
    last_refresh = None

    print("Started training data")
    training_data = "/home/luis.rivera157/p3/SentimentAnalysisDataset.csv"
    conf = SparkConf().setAppName("SentimentAnalysis")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", 'hdfs://master:9000/user/hive/warehouse').enableHiveSupport().getOrCreate()
    data = sc.textFile(training_data)
    header = data.first()
    rdd = data.filter(lambda row: row != header)
    r = rdd.mapPartitions(lambda x: csv.reader(x))
    r = r.map(lambda x: (parse_text(x[3]), int(x[1])))
    r = r.map(lambda x: Row(sentence=x[0], label=int(x[1])))
    df = spark.createDataFrame(r).orderBy(rand()).limit(500000)
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    hashing = HashingTF(numFeatures=10000, inputCol="base_words", outputCol="features")
    regression = LogisticRegression(maxIter=10000, regParam=0.001, elasticNetParam=0.0001)
    pipeline = Pipeline(stages=[tokenizer, stop_words_remover, hashing, regression])
    splits = df.randomSplit([0.6, 0.4], 223)
    train = splits[0]
    model = pipeline.fit(train)

    last_refresh = datetime.now()
    print("Started reading tweets at",last_refresh )
    start_consumer()
