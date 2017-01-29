import pyspark
import sys
import json
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch

conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)

# Input files
textFile = sc.textFile("s3n://timo-twitter-data/2015/05/01/00/30.json")


# Map reduce
def map_func(line):
    each_line = json.loads(line)
    es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)

    if "created_at" in each_line:
        #ret = (each_line['id'], each_line['user']['id'], each_line['timestamp_ms'], each_line['text'] )
        if 'user' in each_line:
            doc = {'usr_id': each_line['user']['id'], 'ttext': each_line['text'], 'time': each_line['timestamp_ms']}
#            es.index(index='test_3', doc_type='inputs', body=doc)
        if 'geo' in each_line:

            print each_line['geo']
    else:
        if 'user' in each_line:
            doc = {'usr_id': each_line['user']['id'], 'ttext': ''}
 #           es.index(index='test_3', doc_type='inputs', body=doc)

def reduce_func(a, b):
    return a + b

counts = textFile.map(map_func)
counts.saveAsTextFile("/tmp/result1")
print "----------------------------------------------"
