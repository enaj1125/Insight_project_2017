mport pyspark
import sys
import json
import datetime, time
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
    A, B, C = False, False, False

    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            A = True
    if 'timestamp_ms' in each_line:
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        B = True
    if 'text' in each_line:
        t_text = each_line['text']
        C = True
    if A and B and C:
        doc = {'usr_id': usr_id, 'ttext': t_text, 'time': t_time}
        #es.index(index='test_3', doc_type='inputs', body=doc)
        print doc

def reduce_func(a, b):
    return a + b

counts = textFile.map(map_func)
counts.saveAsTextFile("/tmp/result1")
print "----------------------------------------------"
