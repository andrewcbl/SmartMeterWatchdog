from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from cassandra.cluster import Cluster
import time

# Setup the the Spark cluster and Cassandra driver
cluster = Cluster()
conf = SparkConf().setAppName("Smarter Meter Online")
conf.set('spark.rdd.compress', 'True')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# HDFS Constants 
HDFS_DIR = "ec2-54-148-53-88.us-west-2.compute.amazonaws.com:9000"
# 50GB dataset
#CAMUS_HF = "/camus/topics/testing_hf1/hourly/2016/02/*/*"
#CAMUS_LF = "/camus/topics/testing_lf1/hourly/2016/02/*/*"
# 200GB dataset
CAMUS_HF = "/camus/topics/house_hf2/hourly/2016/02/*/*"
CAMUS_LF = "/camus/topics/house_lf2/hourly/2016/02/*/*"
# 10GB dataset
#CAMUS_HF = "/camus/topics/testing_hf0/hourly/2016/02/*/*"
#CAMUS_LF = "/camus/topics/testing_lf0/hourly/2016/02/*/*"
# Newer 10GB dataset
#CAMUS_HF = "/camus/topics/testing_hf2/hourly/2016/02/*/*"
#CAMUS_LF = "/camus/topics/testing_lf2/hourly/2016/02/*/*"

# Cassandra constants
CASSANDRA_CLUSTER    = ['52.89.107.2', '54.149.167.208', '52.89.115.218', '54.149.95.1']
CASSANDRA_DB         = 'playground'
CASSANDRA_HF_TABLE   = 'batch_power_main2001'
CASSANDRA_LF_TABLE   = 'batch_power_appliance2001'
CASSANDRA_PCT_TABLE  = 'batch_power_percent2001'
CASSANDRA_GEO_TABLE  = 'batch_power_geographical2001'
CASSANDRA_STAT_TABLE = 'batch_power_stat2001'

JSON_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("houseId", LongType(), True),
    StructField("zip", StringType(), True),
    StructField("readings", ArrayType(StructType([
        StructField("meterId", StringType(), True),
        StructField("power", StringType(), True),
        StructField("label", StringType(), True)
    ])), True)
])

# Functions to support HDFS processing
# TODO: Looks spark.sql already have similar function. Investigate
def ts2date(curTime):
    return time.strftime("%Y-%m-%d", time.localtime(int(curTime)))

def expandRec(rec):
    result = []
    for row in rec.readings:
        result.append((rec.houseId, ts2date(rec.timestamp), int(rec.timestamp), rec.zip, row.label, row.meterId, float(row.power)))
    return result

def getEnergy(readings):
    energy = 0
    readingCnt = len(readings)
    if readingCnt <= 1:
        return ('NONE', 0)
    for i in xrange(readingCnt-1):
        energy += (readings[i+1][0] - readings[i][0]) * readings[i][2]
    return (readings[0][1], energy)

def writeTotalTableToCassandra(agg, tablename):
    if agg:
        cascluster = Cluster(CASSANDRA_CLUSTER)
        casSession = cascluster.connect(CASSANDRA_DB)
        casCommand = ('INSERT INTO %s (houseid, date, zip, label, power) VALUES ' % (tablename)) + '(%s, %s, %s, %s, %s)'
        for rec in agg:
            casSession.execute(casCommand, (str(rec[0][0]), rec[0][3], rec[0][2], rec[0][1], str(rec[1])))
        casSession.shutdown()
        cascluster.shutdown()

def writePercentTableToCassandra(agg, tablename):
    if agg:
        cascluster = Cluster(CASSANDRA_CLUSTER)
        casSession = cascluster.connect('playground')
        casCommand = ('INSERT INTO %s (houseid, date, zip, label, percent) VALUES ' % (tablename)) + '(%s, %s, %s, %s, %s)'
        for rec in agg:
            casSession.execute(casCommand, (str(rec[0]), rec[3], rec[2], rec[1], str(rec[4])))
        casSession.shutdown()
        cascluster.shutdown()

def writeGeoTableToCassandra(agg, tablename):
    if agg:
        cascluster = Cluster(CASSANDRA_CLUSTER)
        casSession = cascluster.connect('playground')
        casCommand = ('INSERT INTO %s (date, zip, power) VALUES ' % (tablename)) + '(%s, %s, %s)'
        for rec in agg:
            casSession.execute(casCommand, (rec[0][1], rec[0][0], str(rec[1])))
        casSession.shutdown()
        cascluster.shutdown()

def writeStatTableToCassandra(agg, tablename):
    if agg:
        cascluster = Cluster(CASSANDRA_CLUSTER)
        casSession = cascluster.connect('playground')
        casCommand = ('INSERT INTO %s (houseid, meterid, percentile) VALUES ' % (tablename)) + '(%s, %s, %s)'
        for rec in agg:
            casSession.execute(casCommand, (str(rec[0]), str(rec[1]), str(rec[2][0])))
        casSession.shutdown()
        cascluster.shutdown()

def j2kwh(energy):
    return energy * 1.0 / 1000 / 3600

# Functions to support generating percentage RDD
def reordRec(rec):
    return ((rec[0][0], rec[0][2], rec[0][3]), (rec[0][1], rec[1]))

def getPercentage(rec):
    if rec is None or rec[1][1][1] < 1e-5:
        percentage = 0.0
    else:
        percentage = rec[1][0][1] * 1.0 / rec[1][1][1]
    return (rec[0][0], rec[1][0][0], rec[0][1], rec[0][2], percentage * 100)

def runHiveStats(rdd):
    rddFilt = rdd.filter(rdd.power > 1e-5)
    hiveContext.registerDataFrameAsTable(rddFilt, "rddTab")
    stats = hiveContext.sql('select houseId,meterId, percentile_approx(power, array(0.97)) from rddTab GROUP BY houseId, meterId')
    stats.foreachPartition(lambda par: writeStatTableToCassandra(par, CASSANDRA_STAT_TABLE))

def processHDFS(hostDir, fileDir, isLf):
#    sourceDf    = sqlContext.read.json("hdfs://" + hostDir + fileDir, JSON_SCHEMA).repartition(4096)
    sourceDf    = sqlContext.read.json("hdfs://" + hostDir + fileDir, JSON_SCHEMA).repartition(4096)
    sourceDfExp = sqlContext.createDataFrame(
        sourceDf.flatMap(lambda row: expandRec(row)), ['houseId', 'date', 'timestamp', 'zip', 'label', 'meterId', 'power']
    )

    sourceDfExp.persist(StorageLevel.MEMORY_AND_DISK)

    runHiveStats(sourceDfExp)

    # key is (houseid, meterid, zip, date)
    # value is list of (timestamp, label, power)
    metersDf = sourceDfExp.map(
        lambda row: ((row.houseId, row.meterId, row.zip, row.date), [(row.timestamp, row.label, row.power)])
    ).reduceByKey(lambda a, b: a + b) # groupByKey()

    # key is (houseid, meterid, zip, date)
    # value is (label, totalEnergy)
    allEnergy = metersDf.map(lambda rec: (rec[0], getEnergy(list(sorted(rec[1])))))

    # key is (houseid, label, zip, date)
    # value is sum of power aggregated by label
    energyTot = allEnergy.map(lambda rec: ((rec[0][0], rec[1][0], rec[0][2], rec[0][3]), rec[1][1])) \
                             .reduceByKey(lambda x, y: x+y)

    energyNorm = energyTot.map(lambda rec: (rec[0], j2kwh(rec[1])))
    return energyNorm

highFreqEnergyNorm = processHDFS(HDFS_DIR, CAMUS_HF, False)
lowFreqEnergyNorm  = processHDFS(HDFS_DIR, CAMUS_LF, True)

highFreqEnergyNorm.cache()
lowFreqEnergyNorm.cache()

lowFreqPercent = (lowFreqEnergyNorm.map(lambda rec: reordRec(rec)) \
                 .leftOuterJoin(highFreqEnergyNorm.map(lambda rec: reordRec(rec)))) \
                 .map(lambda rec: getPercentage(rec))

# Key is (zip, data)
zipTotalPower = highFreqEnergyNorm.map(lambda rec: ((rec[0][2], rec[0][3]), rec[1])).reduceByKey(lambda x, y: x+y)

lowFreqEnergyNorm.foreachPartition(lambda par: writeTotalTableToCassandra(par, CASSANDRA_LF_TABLE))
highFreqEnergyNorm.foreachPartition(lambda par: writeTotalTableToCassandra(par, CASSANDRA_HF_TABLE))
zipTotalPower.foreachPartition(lambda par: writeGeoTableToCassandra(par, CASSANDRA_GEO_TABLE))
lowFreqPercent.foreachPartition(lambda par: writePercentTableToCassandra(par, CASSANDRA_PCT_TABLE))
