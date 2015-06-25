from __future__ import print_function

import uuid
import time
import datetime
import csv

def dumpCoverage2(coverage):
	thisOne=""
	for interval in coverage:
		thisOne = thisOne+"["+str(interval[0])+", "+str(interval[1])+"], "
	thisOne=thisOne[:-2]
	return "["+thisOne+"]"



def dumpCoverage(coverage):
	print("[")
	for interval in coverage:
		print(" [ "+str(interval[0])+", "+str(interval[1])+" ]")
	print("]")

def importData(fname):
	coverage=[]
	try:
		ifile  = open(fname, "r")
		reader = csv.reader(ifile)
		for row in reader:
			if not row[0][0]=="#":
				thisOne=[]
				thisOne.append(int(row[1]))
				thisOne.append(int(row[2]))
				coverage.append(thisOne)
	except Exception as e:
		print ("Error loading test data: "+e.message)

	return coverage

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


def calculateCoverage(testcases, outputFile, t1, t2, interval):
	coverage=[]
	coverage = importData(testcases)
	f = open(outputFile, 'w')

	for thisInterval in range(t1, t2, interval):
		thisCoverage=getCoverage(coverage, thisInterval, thisInterval+interval)
		print(str(thisInterval)+", "+str(thisInterval+interval)+", "+dumpCoverage2(thisCoverage), file = f)

	f.close()

# every 4 hours 4*60*60*1000, starting Jan 01, 2015 at midnight...
calculateCoverage("../data/TestCase1.csv", "../data/4hrCoverage.csv",1420092000000, 1451613600000, 4*60*60*1000)

