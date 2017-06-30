import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import json
from twitter import OAuth, TwitterStream
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/luis.rivera157/spark/spark-2.1.1-bin-hadoop2.7/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar pyspark-shell'


def read_credentials():
    file_name = "credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load "+data_file)
        return None


def start_producer():
    sc = SparkContext(appName="Producer")
    ssc = StreamingContext(sc, 12)
    kds = KafkaUtils.createDirectStream(ssc, ["trump"], {"metadata.broker.list": "136.145.216.152:9092,136.145.216.163:9092,136.145.216.168:9092,136.145.216.173:9092,136.145.216.175:9092"})
    kds.foreachRDD(send)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()


def send(message):
    iterator = twitter_stream.statuses.sample()
    for tweet in iterator:
        if 'id_str' in tweet and tweet['user']['lang'] == "en":
            text = tweet["text"].lower()
            if "trump" in text or "maga" in text or "dictator" in text or "swamp" in text or "drain" in text or "impeach" in text or "comey" in text:
                producer.send('trump', bytes(json.dumps(tweet, indent=6), "ascii"))


if __name__ == "__main__":
    print("Started reading tweets")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    start_producer()
