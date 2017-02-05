 gather friend information
import sys
import json, datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)
import redis

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
        t_time = t_time.strftime('%Y-%m-%d')
        B = True

    if 'retweeted_status' in each_line:
        if 'user' in each_line['retweeted_status']:
            if 'id' in each_line['retweeted_status']['user']:
                retweet_id = each_line['retweeted_status']['user']['id']
                C = True

    if A and B and C:
        return [(retweet_id, usr_id)]
    else:
        return []

# Input files
textFile = sc.textFile("s3n://timo-twitter-data/2015/05/01/01/30.json")
#result1 = textFile.flatMap(map_func).reduceByKey(lambda a, b: a if a<= b else b)
result = textFile.flatMap(map_func).reduceByKey(lambda a, b: a+b)
#result.saveAsTextFile("/tmp/result1")

def save_to_redis(record):
    redis_host = '52.36.25.239'
    redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
    print(type(record), record[0], record[1])
    redis_db.hset("my_first_redis_data", record[0], record[1])
    #redis_db.set("test_1", 1, 5)    

result.foreach(save_to_redis)
