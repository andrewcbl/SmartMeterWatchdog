import pandas as pd
import random
import sys

def readZipCodeDb(zipDbDir):
    df = pd.read_csv(zipDbDir, dtype={'zip': object})
    zipCodeDb= df['zip']
    return zipCodeDb

def getNewZip(num):
    fullZipDb = readZipCodeDb('zipcode_db.csv.full')
    oldZipDb = readZipCodeDb('zipcode_db.csv.old')
    fullZipSet = set(fullZipDb)
    oldZipSet  = set(oldZipDb)
    diff = fullZipSet.difference(oldZipSet)
    return sorted(random.sample(diff, num))

def genNewZipCodeDb(num):
    newZip = getNewZip(num)
    fullZipDf = pd.read_csv('zipcode_db.csv.full', dtype={'zip': object})
    newZipCol = pd.DataFrame(newZip, columns=['zip'])
    newZipDf  = pd.merge(fullZipDf, newZipCol, how='inner', on=['zip'])
    newZipDf.to_csv('zipcode_db.csv')

if __name__ == "__main__":
    genNewZipCodeDb(int(sys.argv[1]))
