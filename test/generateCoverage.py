from __future__ import print_function

import uuid
import time
import datetime
import csv


class coverageObj(object):
	bucket=""
	coverage=[]

	def __init__(self, bucket, coverage):
		self.bucket=bucket
		self.coverage=coverage

def dumpCoverage(coverage):
	thisOne=""
	for interval in coverage:
		thisOne = thisOne+"["+str(interval[0])+", "+str(interval[1])+"], "
	thisOne=thisOne[:-2]
	return "["+thisOne+"]"



def importData(fname):
	retval=[]
	try:
		ifile  = open(fname, "r")
		reader = csv.reader(ifile)
		bucket=""
		coverage=[]
		for row in reader:
			if not row[0][0]=="#":
				if bucket!=row[0]:
					if not bucket=="":
						retval.append(coverageObj(bucket, coverage))
						bucket=row[0]
						coverage=[]
					else:
						bucket = row[0]
						thisOne=[]
						thisOne.append(int(row[1]))
						thisOne.append(int(row[2]))
						coverage.append(thisOne)
				else:
					thisOne=[]
					thisOne.append(int(row[1]))
					thisOne.append(int(row[2]))
					coverage.append(thisOne)

		retval.append(coverageObj(bucket, coverage))
	except Exception as e:
		print ("Error loading test data: "+e.message)

	return retval

def compare(interval, t1,t2):
	retval = []

	if (t1<interval[0]) and (t2>=interval[0]) and (t2<=interval[1]):
		retval.append(interval[0])
		retval.append(t2)
	elif (t1<interval[0]) and (t2>=interval[0]) and (t2>interval[1]):
		retval.append(interval[0])
		retval.append(interval[1])
	elif (t1>=interval[0]) and (t1<=interval[1]) and (t2>=interval[0]) and (t2<=interval[1]):
		retval.append(t1)
		retval.append(t2)
	elif (t1>=interval[0]) and (t1<=interval[1]) and (t2>interval[1]):
		retval.append(t1)
		retval.append(interval[1])
	else:
		retval = None

	return retval


def getCoverage(coverage, t1, t2):
	retval=[]
	
	for interval in coverage:
		thisOne = compare(interval, t1, t2)
		if thisOne is not None:
			retval.append(thisOne)
	return retval



def calculateCoverage(testcases, outputFile, tcName, t1, t2, interval):
	coverage=[]
	coverage = importData(testcases)
	f = open(outputFile, 'w')
	x=1
	for thisID in coverage:
		for thisInterval in range(t1, t2, interval):
			thisCoverage=getCoverage(thisID.coverage, thisInterval, thisInterval+interval)
			print(thisID.bucket+"|"+tcName+"_"+str(x)+"|"+str(thisInterval)+"|"+str(thisInterval+interval)+"|"+dumpCoverage(thisCoverage), file = f)
			x=x+1
	f.close()



# every 4 hours 4*60*60*1000, starting Jan 01, 2015 at midnight...
#calculateCoverage("../data/TestCase1.csv", "../data/TC1_4hrCoverage.csv","TC1_4hrCoverage",1420092000000, 1451613600000, 4*60*60*1000)
# every 8 hours 8*60*60*1000, starting Jan 01, 2015 at midnight...
#calculateCoverage("../data/TestCase1.csv", "../data/TC1_8hrCoverage.csv","TC1_8hrCoverage",1420092000000, 1451613600000, 8*60*60*1000)
# every 12 hours 12*60*60*1000, starting Jan 01, 2015 at midnight...
#calculateCoverage("../data/TestCase1.csv", "../data/TC1_12hrCoverage.csv","TC1_12hrCoverage",1420092000000, 1451613600000, 12*60*60*1000)

# every 24 hours 24*60*60*1000, starting Jan 01, 2015 at midnight...
#calculateCoverage("../data/TestCase1.csv", "../data/TC1_24hrCoverage.csv","TC1_24hrCoverage",1420092000000, 1451613600000, 24*60*60*1000)

# every 48 hours 48*60*60*1000, starting Jan 01, 2015 at midnight...
# calculateCoverage("../data/TestCase1.csv", "../data/TC1_48hrCoverage.csv","TC1_48hrCoverage",1420092000000, 1451613600000, 48*60*60*1000)

# every 72 hours 72*60*60*1000, starting Jan 01, 2015 at midnight...
calculateCoverage("../data/TestCase1.csv", "../data/TC1_72hrCoverage.csv","TC1_72hrCoverage",1420092000000, 1451613600000, 72*60*60*1000)

# every 168 hours 168*60*60*1000, starting Jan 01, 2015 at midnight...
calculateCoverage("../data/TestCase1.csv", "../data/TC1_168hrCoverage.csv","TC1_168hrCoverage",1420092000000, 1451613600000, 168*60*60*1000)

# every 2*168 hours 2*168*60*60*1000, starting Jan 01, 2015 at midnight...
calculateCoverage("../data/TestCase1.csv", "../data/TC1_336hrCoverage.csv","TC1_336hrCoverage",1420092000000, 1451613600000, 336*60*60*1000)

# every 720 hours 720*60*60*1000, starting Jan 01, 2015 at midnight...
calculateCoverage("../data/TestCase1.csv", "../data/TC1_720hrCoverage.csv","TC1_720hrCoverage",1420092000000, 1451613600000, 720*60*60*1000)