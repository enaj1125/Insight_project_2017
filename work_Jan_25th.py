import pyspark
import sys
import json
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)


textFile = sc.textFile("hdfs://ip-172-31-1-12:9000/tmp/30.json")

textFile.printSchema()

def map_func(line):
    each_line = json.loads(line)

    if "created_at" in each_line.keys():
       ret = (each_line['id'], each_line['usr_id'] )
    else:
        ret = (each_line['delete']['status']['id'])

    return ret


def reduce_func(a, b):
    return a + b


counts = textFile.map(map_func).reduce(lambda a, b :a+b)

print counts
                      
