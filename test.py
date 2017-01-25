import pyspark
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-12:7077")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


file = sc.textFile("hdfs://ip-172-31-1-12:9000/tmp/30.json")

#counts = file.flatMap(lambda line: line.split(" "))\
 #          .map(lambda word: (word, 1))\
 #          .reduceByKey(lambda a, b: a + b)

#res = counts.collect()

#for val in res:
#     print val
df = sqlContext.read.json(file)
df.printSchema()
df.select("id").show() 
print "----------------------------------------------"
