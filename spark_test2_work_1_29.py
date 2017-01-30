import pyspark
import sys
import json
import datetime, time
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
from pyspark.sql import SQLContext

# initialize Spark
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Input files: create text file rdd
textFile1 = sc.textFile("s3n://timo-twitter-data/2015/05/01/00/30.json")
#textFile2 = sqlContext.read.json("s3n://timo-twitter-data/2015/05/01/00/30.json")
#textFile2.printSchema()

def map_func(line):
    each_line = json.loads(line)
    if 'timestamp_ms' in each_line:
        if each_line['timestamp_ms'] is not None:
            t_time = float(each_line['timestamp_ms'][:10])
            print each_line['timestamp_ms']
            x = datetime.datetime.utcfromtimestamp(t_time)
            print(x)
   # print each_line
   # if 'coordinates' in each_line:
   #     print each_line['coordinates']['coordinates']
   # if 'place' in each_line:
   #     print each_line['bounding_box']['coordinates']
   # if 'geo' in each_line:
   #     print each_line['geo']['coordinates']


lines = textFile1.map(map_func)
lines.saveAsTextFile("/tmp/result2")


#es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)
#doc = {'timeStamp': 201709021230, 'wind': 20, 'wban': 528}
#es.index(index='test_1', doc_type='inputs', body=doc )
