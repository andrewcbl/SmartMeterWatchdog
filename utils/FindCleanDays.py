import sys
import time
import datetime

houseName = str(sys.argv[1])
commonFh  = open(houseName + '/common.dat', 'w')

tsInDay = 86400

def findCleanDays(filename):
    result = []

    origFh = open(filename)
    
    startTs = int(time.mktime(datetime.datetime.strptime(sys.argv[2], "%m/%d/%Y:%H:%M:%S").timetuple()))
    startTsP1 = startTs + tsInDay
    prevBatchTs = None
    prevTs = None
    curBatch = []
    needDrop = False
    
    for line in origFh:
        curTs, reading = line.split()
    
        if int(curTs) < startTs:
            continue
    
        if int(curTs) >= startTsP1:
            if (int(startTsP1) - int(prevTs) < 1000):
                if not needDrop:
                    print "Saving curTs = " + str(curTs) + " " + "startTs = " + str(startTs) + " startTsP1 = " + str(startTsP1) + " " + str(len(curBatch))
                    result.append(startTs)
            while int(startTsP1) <= int(curTs):
                startTs = startTsP1
                startTsP1 = startTs + tsInDay
            curBatch = []
            curBatch.append((curTs, reading))
            prevBatchTs = prevTs
#            print "Checking curTs = " + str(curTs) + " " + "startTs = " + str(startTs) + " startTsP1 = " + str(startTsP1) + " " + str(needDrop)
            if int(curTs) - startTs > 1000:
                needDrop = True
            else:
                needDrop = False
        else:
            if prevTs is not None and int(curTs) - int(prevTs) > 1000:
                print "prevTs = " + str(prevTs) + " curTs = " + str(curTs)
                needDrop = True
            curBatch.append((curTs, reading))
    
        prevTs = curTs
    origFh.close()

    return result

lfCleanDays = findCleanDays(houseName + '/channel_1.dat')
print "-------------------------"
hfCleanDays = findCleanDays(houseName + '/channel_3.dat')
daysIntersection = list(set(lfCleanDays).intersection(hfCleanDays))
daysIntersection.sort()

print "Low Frequency days"
for day in lfCleanDays:
    print str(day)

print "High Frequency days"
for day in hfCleanDays:
    print str(day)

print "Intersection: " + str(len(daysIntersection))
for day in daysIntersection:
    commonFh.write(str(day) + '\n')
