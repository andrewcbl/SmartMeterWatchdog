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
        self.hfTimestamp = None
        self.lfTimestamp = None
        self.hfDone = False
        self.lfDone = False
        self.hfLine = 1
        self.lfLine = 1

    def getRecordAgg(self, dataDir, houseId, lineNum, startChannel, endChannel):
        resultReadings = ()
        resultTimestamp = None

        for i in xrange(startChannel, endChannel + 1):
            sourceData = dataDir + 'house_' + str(houseId) + '/channel_' + str(i) + '.dat'
#            print "lineNum = " + str(lineNum) + "sourceData = " + sourceData
            curReading = linecache.getline(sourceData, lineNum)
            timestamp, reading = curReading.split()

            if i == startChannel:
                resultTimestamp = timestamp

            resultReadings = resultReadings + \
                           ({'meterId': str(i),
                             'label': self.meterDict[str(i)],
                             'power': reading},)

        return (resultTimestamp, resultReadings)

    def houseSentDone(self):
        return self.lfDone and self.hfDone

    def getRecord(self):
        recordIsLf = None

#        print "lfDone = " + str(self.lfDone) + " hfDone = " + str(self.hfDone) + " numLfRecs " + str(self.houseConfig.numLfRecs) + " numHfRecs " + str(self.houseConfig.numHfRecs)

        if self.hfTimestamp is None and self.lfTimestamp is None:
            recordIsLf = True
        elif self.hfTimestamp is None:
            recordIsLf = False
        elif self.lfTimestamp is None:
            recordIsLf = False
        elif self.houseSentDone():
            assert "ERROR!"
        elif self.lfDone:
            recordIsLf = False
        elif self.hfDone:
            recordIsLf = True
        else:
            recordIsLf = self.lfTimestamp < self.hfTimestamp

        if recordIsLf:
            lineNum = self.lfLine
            self.lfLine += 1
            startChannel = 3
            endChannel = self.houseConfig.numMeters

            if self.lfLine > self.houseConfig.numLfRecs:
                self.lfDone = True
        else:
            lineNum = self.hfLine
            self.hfLine += 1
            startChannel = 1
            endChannel = 2
            if self.hfLine > self.houseConfig.numHfRecs:
                self.hfDone = True

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

    def __init__(self, startHouseId, endHouseId, houseStatus, dataDir, zipDbDir):
        self.dataDir      = dataDir
        self.tracker      = {}
        self.meterDicts   = {}
        self.houseNumRecs = {}
        self.houseStatus  = houseStatus

        self.availHouse   = set(range(startHouseId, endHouseId+1))

        self.initZipCodeDb(zipDbDir)
        self.initHouseConfig(dataDir)
        self.houseHist    = self.readHouseStatus(houseStatus)

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

    def readHouseStatus(self, houseStatus):
        houseFh = open(houseStatus).readlines()

        houseHist = {}

        for line in houseFh:
            (houseId, sHouseId, zipCode, scaleFactor) = line.split()
            houseHist[int(houseId)] = (int(sHouseId), zipCode, float(scaleFactor))

        return houseHist

    def writeHouseStatus(self):
        houseFh = open(self.houseStatus + ".new", 'w')

        for houseId in self.houseHist.keys():
            rec = self.houseHist[houseId]
            houseFh.write("%s %s %s %s\n" % (houseId, rec[0], rec[1], rec[2]))

        houseFh.close()

    def initZipCodeDb(self, zipDbDir):
        df = pd.read_csv(zipDbDir, dtype={'zip': object})

        self.zipCodeDb= df['zip']
        self.zipCodeCnt = len(self.zipCodeDb)

    def initHouseConfig(self, dataDir):
        for i in xrange(1, 7):
            self.meterDicts[i] = self.readLabels(dataDir, i)
            hfData = dataDir + 'house_' + str(i) + '/channel_1.dat'
            lfData = dataDir + 'house_' + str(i) + '/channel_3.dat'
            self.houseNumRecs[i] = (self.fileLen(lfData), self.fileLen(hfData))

    def houseSentDone(self):
        return len(self.availHouse) == 0

    def getRecord(self):
        if (len(self.availHouse) > 0):
            houseId = random.sample(self.availHouse, 1)[0]
        else:
## TODO: Raise exception for this
            return None

        if houseId not in self.tracker.keys():
            sHouseId = None
            scaleFactor = None
            zipCode = None

            if houseId in self.houseHist.keys():
                (sHouseId, zipCode, scaleFactor) = self.houseHist[houseId]
            else:
                sHouseId = random.randint(1, 5)
                sHouseId = 6
                scaleFactor = (random.random() * 2 - 1) * 0.2 + 1
                zipCode = self.zipCodeDb[random.randint(0, self.zipCodeCnt - 1)]

                self.houseHist[houseId] = (sHouseId, zipCode, scaleFactor)
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

        record = self.tracker[houseId].getRecord()

        if (self.tracker[houseId].houseSentDone()):
            self.availHouse.remove(houseId)

        return record
