import findspark
findspark.init()
from pyspark.sql import Row, SparkSession
from datetime import datetime, timedelta


def write_keywords(spark):
    try:
        keywords_data_frame = spark.sql("select * from tweets")
        keywords_rdd = keywords_data_frame.rdd
        keywords_rdd = keywords_rdd.filter(lambda x: x["timestamp"] > datetime.now() - timedelta(hours=1))
        keywords_data_frame = spark.createDataFrame(keywords_rdd.map(lambda x: Row(sentence=x["sentence"])))
        global model
        result = model.transform(keywords_data_frame)
        positive_count = result.where('prediction == 1').count()
        negative_count = result.where('prediction == 0').count()
        now = datetime.now()
        result_dict = {"positive": positive_count, "negative": negative_count, "timestamp": now}
        fi = open('/home/luis.rivera157/p3/bigdata-project3/visualizations/keywords.txt', 'a')
        fi.write(str(result_dict))
        fi.write("\n")
        fi.close()
        print("Record appended to file")
    except:
        print("Error writing in file")
        pass