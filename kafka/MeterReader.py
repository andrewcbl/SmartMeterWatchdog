import linecache
import random
import json
import subprocess
import pandas as pd

class HouseConfig(object):
    def __init__(self, sHouseId, houseId, sDataDir, zipCode, scaleFactor, numMeters, numLfRecs, numHfRecs):
        self.sHouseId    = sHouseId
        self.houseId     = houseId
        self.sDataDir    = sDataDir
        self.zipCode     = zipCode
        self.scaleFactor = scaleFactor
        self.numLfRecs   = numLfRecs
        self.numHfRecs   = numHfRecs
        self.numMeters   = numMeters

class MeterTracker(object):

    def __init__(self, houseConfig, meterDict):
        self.houseConfig = houseConfig;
        self.sMeters    = {}
        self.meterDict  = meterDict
        self.fileLength = 0
        self.hfTimestamp = None
        self.lfTimestamp = None

    def getRecordAgg(self, dataDir, houseId, lineNum, startChannel, endChannel):
        resultReadings = ()
        resultTimestamp = None

        for i in xrange(startChannel, endChannel + 1):
            sourceData = dataDir + 'house_' + str(houseId) + '/channel_' + str(i) + '.dat'
            curReading = linecache.getline(sourceData, lineNum)
            timestamp, reading = curReading.split()

            if i == startChannel:
                resultTimestamp = timestamp

            resultReadings = resultReadings + \
                           ({'meterId': str(i),
                             'label': self.meterDict[str(i)],
                             'power': reading},)

        return (resultTimestamp, resultReadings)

    def getRecord(self):
        recordIsLf = None

        if self.hfTimestamp is None and self.lfTimestamp is None:
            recordIsLf = True
        elif self.hfTimestamp is None:
            recordIsLf = False
        elif self.lfTimestamp is None:
            recordIsLf = False
        else:
            recordIsLf = self.lfTimestamp < self.hfTimestamp

        meterId = random.randint(1, len(self.meterDict))
        if meterId not in self.sMeters:
            self.sMeters[meterId] = 1

        lineNum = self.sMeters[meterId]

        if lineNum > self.fileLength:
            lineNum = 1

        self.sMeters[meterId] = lineNum + 1

        if not recordIsLf:
            startChannel = 1
            endChannel = 2
        else:
            startChannel = 3
            endChannel = self.houseConfig.numMeters

        (timestamp, readings) = self.getRecordAgg(self.houseConfig.sDataDir, 
                                                  self.houseConfig.sHouseId,
                                                  lineNum,
                                                  startChannel,
                                                  endChannel)

        if recordIsLf:
            self.lfTimestamp = timestamp
        else:
            self.hfTimestamp = timestamp

        msg = json.dumps({'timestamp': timestamp,
                          'zip': self.houseConfig.zipCode,
                          'houseId': self.houseConfig.houseId,
                          'readings': readings})
        return (recordIsLf, msg)

class MeterLfReader(object):

    def __init__(self, numHouse, dataDir, zipDbDir):
        self.dataDir      = dataDir
        self.numHouse     = numHouse
        self.tracker      = {}
        self.meterDicts   = {}
        self.houseNumRecs = {}

        self.initZipCodeDb(zipDbDir)
        self.initHouseConfig(dataDir)

    def fileLen(self, fname):
        p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        result, err = p.communicate()

        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0])

    def readLabels(self, sDataDir, houseId):
        labelPath = sDataDir + '/house_' + str(houseId) + '/labels.dat'
        labelFh = open(labelPath).readlines()
        meterDict = {}

        for line in labelFh:
            meterId, label = line.split()
            meterDict[meterId] = label

        return meterDict

    def initZipCodeDb(self, zipDbDir):
        df = pd.read_csv(zipDbDir, dtype={'zip': object})

        self.zipCodeDb= df['zip']
        self.zipCodeCnt = len(self.zipCodeDb)

    def initHouseConfig(self, dataDir):
        for i in xrange(1, 7):
            self.meterDicts[i] = self.readLabels(dataDir, i)
            lfData = dataDir + 'house_' + str(i) + '/channel_1.dat'
            hfData = dataDir + 'house_' + str(i) + '/channel_3.dat'
            self.houseNumRecs[i] = (self.fileLen(lfData), self.fileLen(hfData))

    def getRecord(self):
        houseId = random.randint(0, self.numHouse - 1)

        if houseId not in self.tracker.keys():
            sHouseId = random.randint(1, 6)
            zipCode = self.zipCodeDb[random.randint(0, self.zipCodeCnt - 1)]
            scaleFactor = random.random() * 1.5
            houseConfig = HouseConfig(sHouseId, 
                                      houseId, 
                                      self.dataDir, 
                                      zipCode, 
                                      scaleFactor, 
                                      len(self.meterDicts[sHouseId]) - 2, 
                                      self.houseNumRecs[sHouseId][0], 
                                      self.houseNumRecs[sHouseId][1])
            meterTracker = MeterTracker(houseConfig, self.meterDicts[sHouseId])
            self.tracker[houseId] = meterTracker

        return self.tracker[houseId].getRecord()
