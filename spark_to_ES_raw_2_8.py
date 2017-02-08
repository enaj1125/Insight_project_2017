# write data into ES
import sys
import json
import datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-4:7077")
sc = SparkContext(conf = conf)
from boto.s3.connection import S3Connection

# Elasticsearch setting
ES_NODES = 'ec2-35-163-59-141.us-west-2.compute.amazonaws.com'
ES_INDEX = 'twitter_data_may'
ES_TYPE = 'inputs'
ES_RESOURCE = '/'.join([ES_INDEX,ES_TYPE])
es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':'elastic','es.net.http.auth.pass':'changeme'}
es = Elasticsearch(['ip-172-31-1-11'], http_auth=('elastic', 'changeme'), verify_certs=False)

# Map reduce

def map_func(line):
    each_line = json.loads(line)
    A, B, C = False, False, False
    retweet_id = []

    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            A = True
    if 'timestamp_ms' in each_line:
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        t_time = t_time.strftime('%Y-%m-%d')
        B = True

    if 'text' in each_line:
        t_text = each_line['text']
        C = True

    if A and B and C:
        doc = {'usr_id': usr_id, 'ttext': t_text, 'ttimes': t_time}

        return [('key', doc)]
    else:
        return []


textfiles = sc.textFile("s3n://timo-twitter-data/2015/05/*/*/*.json")

result1 = textfiles.flatMap(map_func)

result1.saveAsNewAPIHadoopFile(path='-', \
                                            outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
                                            keyClass='org.apache.hadoop.io.NullWritable', \
                                            valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
                                            conf=es_conf)
                                                                                                                                                                                                                              49,0-1        B
