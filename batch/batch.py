from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
import time

# Setup the the Spark cluster and Cassandra driver
cluster = Cluster()
conf = SparkConf().setAppName("Smart Meter Watchdog")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# HDFS Constants 
HDFS_DIR = "ec2-52-34-246-104.us-west-2.compute.amazonaws.com:9000"
CAMUS_HF = "/camus/topics/house_hf/hourly/2016/02/02/*"
CAMUS_LF = "/camus/topics/house_lf/hourly/2016/02/02/*"
#HDFS_DIR = "ec2-52-89-146-18.us-west-2.compute.amazonaws.com:9000/"
#CAMUS_HF = "/camus/topics/testing_hf/hourly/2016/01/31/14"
#CAMUS_LF = "/camus/topics/testing_lf/hourly/2016/01/31/14"

# Cassandra constants
CASSANDRA_CLUSTER   = ['52.89.146.18', '52.89.197.150', '52.89.235.220', '52.89.249.32']
CASSANDRA_DB        = 'playground'
CASSANDRA_HF_TABLE  = 'batch_power_main'
CASSANDRA_LF_TABLE  = 'batch_power_appliance'
CASSANDRA_PCT_TABLE = 'batch_power_percent'
CASSANDRA_GEO_TABLE = 'batch_power_geographical'

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

def j2kwh(energy):
    return energy * 1.0 / 1000 / 3600

# Functions to support generating percentage RDD
def reordRec(rec):
    return ((rec[0][0], rec[0][2], rec[0][3]), (rec[0][1], rec[1]))

def getPercentage(rec):
    if rec[1][1][1] < 1e-5:
        percentage = 0.0
    else:
        percentage = rec[1][0][1] * 1.0 / rec[1][1][1]
    return (rec[0][0], rec[1][0][0], rec[0][1], rec[0][2], percentage * 100)

def processHDFS(hostDir, fileDir, isLf):
#    sourceDf    = sqlContext.read.json("hdfs://" + hostDir + fileDir).repartition(72)
    sourceDf    = sqlContext.read.json("hdfs://" + hostDir + fileDir)
    sourceDfExp = sqlContext.createDataFrame(
        sourceDf.flatMap(lambda row: expandRec(row)), ['houseId', 'date', 'timestamp', 'zip', 'label', 'meterId', 'power']
    )

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

lowFreqPercent = (lowFreqEnergyNorm.map(lambda rec: reordRec(rec)) \
                 .leftOuterJoin(highFreqEnergyNorm.map(lambda rec: reordRec(rec)))) \
                 .map(lambda rec: getPercentage(rec))

# Key is (zip, data)
zipTotalPower = highFreqEnergyNorm.map(lambda rec: ((rec[0][2], rec[0][3]), rec[1])).reduceByKey(lambda x, y: x+y)

lowFreqEnergyNorm.foreachPartition(lambda par: writeTotalTableToCassandra(par, CASSANDRA_LF_TABLE))
highFreqEnergyNorm.foreachPartition(lambda par: writeTotalTableToCassandra(par, CASSANDRA_HF_TABLE))
lowFreqPercent.foreachPartition(lambda par: writePercentTableToCassandra(par, CASSANDRA_PCT_TABLE))
zipTotalPower.foreachPartition(lambda par: writeGeoTableToCassandra(par, CASSANDRA_GEO_TABLE))
