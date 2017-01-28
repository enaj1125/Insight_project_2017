import pyspark
import sys
import json
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch

conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)

# Input files
textFile = sc.textFile("hdfs://ip-172-31-1-12:9000/tmp/30.json")

# set elasticsearch
es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)

# Map reduce
def map_func(line):
    each_line = json.loads(line)
    es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)

    if "created_at" in each_line.keys():
        #ret = (each_line['id'], each_line['user']['id'], each_line['timestamp_ms'], each_line['text'] )
        if 'user' in each_line:
            doc = {'usr_id': each_line['user']['id'], 'ttext': each_line['text']}
            es.index(index='test_2', doc_type='inputs', body=doc)
    else:
        if 'user' in each_line:
            doc = {'usr_id': each_line['user']['id'], 'ttext': ''}
            es.index(index='test_2', doc_type='inputs', body=doc)
    #return ret

def reduce_func(a, b):
    return a + b

counts = textFile.map(map_func)
counts.saveAsTextFile("/tmp/result")
print "----------------------------------------------"

# write into Elasticsearch
es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)
doc = {'timeStamp': 201709021230, 'wind': 20, 'wban': 528}
es.index(index='test_1', doc_type='inputs',body=doc )
