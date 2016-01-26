from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

from cassandra.cluster import Cluster

import time
import json

sc = SparkContext(appName="StreamingKafkaLowFrequency")
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def ts2date(curTime):
    return time.strftime("%D", time.localtime(int(curTime)))

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rdd_date = rdd.map(lambda w: Row(houseId=str(json.loads(w)["houseId"]), date=str(ts2date(json.loads(w)['timestamp'])), zip=str(json.loads(w)["zip"]), power=str(json.loads(w)["power"])))
    rdd_aggr = rdd_date.map(lambda x: ((x.houseId, x.date, x.zip), x.power)).reduceByKey(lambda x, y: float(x)+float(y))
    house_clean = rdd_aggr.map(lambda x: {
        "houseId": x[0][0],
        "date": x[0][1],
        "zip": x[0][2],
        "power": x[1]
    })
    house_clean_df = sqlContext.createDataFrame(house_clean)
    house_clean_df.foreachPartition(aggToCassandra)

def aggToCassandra(agg):
    if agg:
        cascluster = Cluster(['52.89.47.199', '52.89.59.188', '52.88.228.95', '52.35.74.206'])
        casSession = cascluster.connect('playground')
        for rec in agg:
            casSession.execute('INSERT INTO power_aggr_rt (houseId, date, zip, power) VALUES (%s, %s, %s, %s)', (str(rec[0]), str(rec[1]), str(rec[2]), str(rec[3])))
        casSession.shutdown()
        cascluster.shutdown()

kafkaStream = KafkaUtils.createStream(ssc, "ec2-52-35-74-206.us-west-2.compute.amazonaws.com:2181", "smw_streaming", {"smw_low_freq7": 1})
power_rt = kafkaStream.map(lambda rec: rec[1])

power_rt.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
