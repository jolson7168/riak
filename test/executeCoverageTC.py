#!/usr/bin/env python



from __future__ import print_function      # get latest print functionality

###################################################
# Python standard modules (libraries)

import os.path
import math
import functools 
import sys
import time               # sleep function for connection back-off
import json               # JSON object encoding/decoding
import logging
import csv
import ast

import riakLib
from ConfigParser import RawConfigParser

###################################################
# Third party modules (libraries)
#
from riak import RiakClient
from riak import RiakObject
###################################################
# File global variables
#
version_info = ('0', '5', '0')     # Major, minor, patch
g_app_name = 'executeCoverageTC'
g_app_version = '.'.join(version_info)

g_end_prev = time.time()           # end time previous time

def compareArrays(gotArray, expectedString):
	if expectedString.strip()=="[]":
		newArray=[]
	else:
		newArray=ast.literal_eval(expectedString)
	if newArray == gotArray:
		return "PASS"
	else:
		return "FAIL"

def dumpCoverage(coverage):
	thisOne=""
	for interval in coverage:
		thisOne = thisOne+"["+str(interval[0])+", "+str(interval[1])+"], "
	thisOne=thisOne[:-2]
	return "["+thisOne+"]"

def currentDayStr():
    return time.strftime("%Y%m%d")

def currentTimeStr():
    return time.strftime("%H:%M:%S")

def initLog():
    logger = logging.getLogger(cfg.get('logging', 'logname'))
    hdlr = logging.FileHandler(cfg.get('logging', 'logFile'))
    formatter = logging.Formatter(cfg.get('logging', 'logFormat'),cfg.get('logging', 'logTimeFormat'))
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    return logger

def getTestCases(tc):
	cases=[]
	for eachTC in tc.split(","):
		cases.append(eachTC)
	return cases

def executeTestCase(riak, test):
	#try:
	ifile  = open(test, "r")
	reader = csv.reader(ifile,delimiter='|')
	for row in reader:
		startTime = time.time()
		results = riakLib.calculateCoverage(riak, row[0],int(row[1]),int(row[2]))
		duration = round((time.time() - startTime),3)
		results = compareArrays(results, row[3])
		logger.info("Dur: "+str(duration)+" Results: "+ results)
	#except Exception as e:
	#	print ("Error loading test data: "+e.message)

def getCmdLineParser():
    import argparse
    desc = 'Execute coverage test cases'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-f', '--file', default='../config/coverageTestCases_config',
                        help='optional configuration file name (*.ini format)')

    return parser

if __name__ == '__main__':

    p = getCmdLineParser()
    args = p.parse_args()

    cfg = RawConfigParser()
    cfg.read(args.file)
 
    logger = initLog()
    logger.info('Starting Run: '+currentDayStr()+'  ==============================')

    riakIP = cfg.get('riak', 'cluster')
    riakPort = cfg.get('riak', 'port')
    riak = riakLib.configureRiak(riakIP, riakPort,logger)
    testCases = getTestCases(cfg.get('app', 'testfiles'))
    for test in testCases:
    	executeTestCase(riak, test)
    	

    logger.info('Ending Run: '+currentDayStr()+'  ==============================')






	





