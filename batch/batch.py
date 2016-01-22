from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *

from cassandra.cluster import Cluster
cluster = Cluster()

ec2_host = "ec2-52-35-74-206.us-west-2.compute.amazonaws.com:9000/"
hdfs_dir = "camus/topics/smw_low_freq2/hourly/2016/01/21/00"

conf = SparkConf().setAppName("Smart Meter Watchdog")
sc = SparkContext(conf=conf) 
sqlContext = SQLContext(sc)
df = sqlContext.read.json("hdfs://" + ec2_host + hdfs_dir)

# Displays the content of the DataFrame to stdout
df.show()



#session = cluster.connect('playground')
#
#result = session.execute("select * from email")
#for x in result: print x
