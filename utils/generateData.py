import sys
import time
import datetime
import random

houseName = str(sys.argv[1])
commonFh = open(houseName + '/common.dat')
cleanDays = commonFh.readlines()

def writeBatch(newFh, batch, delta, shift, var):
    for (ts, reading) in batch:
        newReading = float(reading) * var
        str = '%s %.2f' % (int(ts) + delta + shift, newReading)
        newFh.write(str + '\n')

tsInDay = 86400

def readBatches(dataFh):
    ptr = 0
    
    startTs = int(cleanDays[ptr])
    startTsP1 = startTs + tsInDay

    curBatch = []
    allBatch = []
    
    for line in dataFh.readlines():
        (timestamp, reading) = line.split()
        if (int(timestamp) < startTs):
            continue
        if (int(timestamp) < startTsP1):
            curBatch.append((timestamp, reading))
        if (int(timestamp) >= startTsP1):
            print ptr
            allBatch.append(curBatch)
            curBatch = []
            curBatch.append((timestamp, reading))
            ptr += 1
            if (ptr >= len(cleanDays)):
                break;
            startTs = int(cleanDays[ptr])
            startTsP1 = startTs + tsInDay

    return allBatch

def rounded(timestamp):
    return (timestamp / tsInDay) * tsInDay

def process(fileIdx, shift, var):
    channelName = '/channel_%d.dat' % fileIdx
    dataFh = open(houseName + channelName)
    dataFhNew = open(houseName + channelName + '.new', 'w')

    allBatch = readBatches(dataFh)
    startTs = int(cleanDays[0])
    
    days = 0
    batchPtr = 0
    
    while days < 30:
        curStartTs = startTs + days * tsInDay
        curBatch = allBatch[batchPtr]
        sts = curBatch[batchPtr][0]
        delta = curStartTs - rounded(int(sts))
        print str(batchPtr) + " batchLen = " + str(len(curBatch)) + " delta = " + str(delta)
        batchPtr = (batchPtr + 1) % len(cleanDays)
        writeBatch(dataFhNew, curBatch, delta, shift, var)
        days += 1
    
    totalRec = 0
    
    for batch in allBatch:
        totalRec += len(batch)
    
    print "totalRec = " + str(totalRec)
    
    dataFh.close()
    dataFhNew.close()

numChannels = int(sys.argv[2])

if (len(sys.argv) > 3):
    shift = int(sys.argv[3])
else:
    shift = 0

for i in range(1, numChannels+1):
    var = (random.random() * 2 - 1) * 0.05 + 1
    process(i, shift, var)

commonFh.close()
