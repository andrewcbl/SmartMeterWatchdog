from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

from cassandra.cluster import Cluster

from tdigest import TDigest
import rethinkdb as r

import time
import json

# Rethink db connection parameters
rdb_dns    = "ec2-52-89-146-18.us-west-2.compute.amazonaws.com"
rdb_port   = 28015

# Kafka streaming connection parameters
kafka_dns  = "ec2-52-35-196-134.us-west-2.compute.amazonaws.com"
kafka_port = "2181"

sc = SparkContext(appName="StreamingKafkaLowFrequency")
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)

def connectRethinkDb():
    r.connect(host=rdb_dns, port=rdb_port, db="test").repl()

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# Functions to support HDFS processing
# TODO: Looks spark.sql already have similar function. Investigate
def ts2date(curTime):
    return time.strftime("%Y-%m-%d", time.localtime(int(curTime)))

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rdd_date = rdd.map(lambda w: Row(houseId=str(json.loads(w)["houseId"]), date=str(ts2date(json.loads(w)['timestamp'])), zip=str(json.loads(w)["zip"]), power=str(float(json.loads(w)["readings"][0]['power']) + float(json.loads(w)["readings"][1]['power']))))
    rdd_aggr = rdd_date.map(lambda x: ((x.houseId, x.date, x.zip), x.power)).reduceByKey(lambda x, y: float(x)+float(y))
    house_clean = rdd_aggr.map(lambda x: {
        "houseId": x[0][0],
        "date": x[0][1],
        "zip": x[0][2],
        "power": x[1]
    })
    house_clean_df = sqlContext.createDataFrame(house_clean)
#    house_clean_df.foreachPartition(aggToCassandra)
    house_clean_df.foreachPartition(aggToRethinkdb)

def updateRecord(rec):
    try:
        conn = r.connect(host=rdb_dns, port=rdb_port, db="test")
    except RqlDriverError:
        abort(503, "No database connection could be established.")

#    rec = json.dumps({'housid:': str(rec[0]),
#                      'date:': str(rec[1]),
#                      'zip:': str(rec[2]),
#                      'power:': str(rec[3])})
#
#    r.table('power_aggr_rt').insert(rec).run()

    r.table("power_aggr_rt").insert([{
        "houseid:": str(rec[1]),
        "date:": str(rec[0]),
        "zip:": str(rec[3]),
        "power": str(rec[2])
    }]).run(conn)

    conn.close()

def aggToRethinkdb(agg):
    if agg:
        for rec in agg:
            updateRecord(rec)

def aggToCassandra(agg):
    if agg:
        cascluster = Cluster(['52.89.47.199', '52.89.59.188', '52.88.228.95', '52.35.74.206'])
        casSession = cascluster.connect('playground')
        for rec in agg:
            casSession.execute('INSERT INTO power_aggr_rt (houseId, date, zip, power) VALUES (%s, %s, %s, %s)', (str(rec[0]), str(rec[1]), str(rec[2]), str(rec[3])))
        casSession.shutdown()
        cascluster.shutdown()

# connectRethinkDb()

kafkaStreamHf = KafkaUtils.createStream(ssc, kafka_dns + ":" + kafka_port, "smw_streaming", {"testing_streaming_hf": 1})
power_rt_hf = kafkaStreamHf.map(lambda rec: rec[1])

power_rt_hf.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
