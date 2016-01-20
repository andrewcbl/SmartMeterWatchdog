import linecache
import random
import json
import subprocess
import pandas as pd

class MeterTracker(object):

    def __init__(self, sHouseId, sDataDir, zipCode):
        self.sHouseId = sHouseId
        self.sMeters = {}
        self.sDataDir = sDataDir
        self.meterDict = {}
        self.zipCode = zipCode
        self.readLabels()

    def fileLen(self, fname):
        p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        result, err = p.communicate()

        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0])

    def readLabels(self):
        labelPath = self.sDataDir + '/house_' + str(self.sHouseId) + '/labels.dat'
        labelFh = open(labelPath).readlines()

        for line in labelFh:
            meterId, label = line.split()
            self.meterDict[meterId] = label

    def getRecord(self):
        meterId = random.randint(1, len(self.meterDict))
        if meterId not in self.sMeters:
            self.sMeters[meterId] = 1

        lineNum = self.sMeters[meterId]

        sourceData = self.sDataDir + 'house_' + str(self.sHouseId) + '/channel_' + str(meterId) + '.dat'

        if lineNum > self.fileLen(sourceData):
            lineNum = 1

        curReading = linecache.getline(sourceData, lineNum)
        timestamp, reading = curReading.split()

        self.sMeters[meterId] = lineNum + 1

        msg = json.dumps({'timestamp': timestamp,
                          'zip': self.zipCode,
                          'houseId': self.sHouseId,
                          'meterId': meterId,
                          'label': self.meterDict[str(meterId)],
                          'power': reading})
        return msg

class MeterLfReader(object):

    def __init__(self, numHouse, dataDir, zipDbDir):
        self.dataDir = dataDir
        self.numHouse = numHouse
        self.tracker = {}

        df = pd.read_csv(zipDbDir, dtype={'zip': object})

        self.zipcode_db = df['zip']
        self.zipcode_cnt = len(self.zipcode_db)

    def getRecord(self):
        houseId = random.randint(0, self.numHouse - 1)

        if houseId not in self.tracker.keys():
            sHouseId = random.randint(1, 6)
            zipCode = self.zipcode_db[random.randint(0, self.zipcode_cnt - 1)]
            meterTracker = MeterTracker(sHouseId, self.dataDir, zipCode)
            self.tracker[houseId] = meterTracker
            self.tracker[houseId].getRecord()

        return self.tracker[houseId].getRecord()
