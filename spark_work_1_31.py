import pyspark
import sys
import json
import datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-4:7077")
sc = SparkContext(conf = conf)
from boto.s3.connection import S3Connection

# Input files
textFile1 = sc.textFile("s3n://timo-twitter-data/2015/05/01/00/30.json")

# Elasticsearch setting
es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)
ES_INDEX = "twitter_indice"
def create_es_index():
    es_mapping = {"yan_type": { "properties":{"usr_id": {"type":"text"}, 'ttext':{"type":"text"}, 'ttimes': {"type":"date"}} } }
    es_settings = {'number_of_shards':3, 'number_of_replicas': 2, 'refresh_interval': '1s', 'index.translog.flush_threshold_size': '1gb'}
    ES_indice = es.indices.create(index = ES_INDEX, body = {'settings': es_settings, 'mappings': es_mapping})
if not es.indices.exists(ES_INDEX):
    create_es_index()

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
        t_time = t_time.strftime('%m/%d/%Y')
        B = True
    if 'text' in each_line:
        t_text = each_line['text']
        C = True
    if A and B and C:
        doc = {'usr_id': usr_id, 'ttext': t_text, 'ttimes': t_time}
    #    es.index(index= ES_INDEX, doc_type='inputs', body=doc)
        return (usr_id, [doc])
    else:
        return ('000', [])

def reduce_func(a, b):
    return a + b
    
# Collect all input files into one giant
conn = S3Connection('AKIAIZ7SQS7534GVEX5A','bUYTVSE5o/k6culnDV/Pdto9U5HVJT/MFRArYnGx')
bucket = conn.get_bucket('timo-twitter-data')
the_path = "s3n://timo-twitter-data/"

data_collection = []
i = 0
for filename in bucket:
    # read data
    i += 1
    # print "this is i", i 
    fullpath = the_path + filename.name
    if i < 10:
        rdd = sc.textFile(fullpath)

    # send to map for filtering 
        result = rdd.map(map_func)
        print result
    # make file namei
        tmp_file = "/tmp/my_tmp_result"+ str(i)
        result.saveAsTextFile(tmp_file)

    # append result together
        data_collection.append(result)


print len(data_collection)

print "----------------------------------------------"
