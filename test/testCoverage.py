import csv
import json
import logging
import time

from riak import RiakClient
from riak import RiakObject
from ConfigParser import RawConfigParser

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

def modifyTestData(riak, action, testFile):
	try:
		ifile  = open(testFile, "rb")
		reader = csv.reader(ifile)
		for row in reader:
			if not row[0][0]=="#":
					modifyBlocks(riak, action, row[0],int(row[1]),int(row[2]),int(row[3]),row[4])
	except Exception as e:
		logger.error("Error loading test data: "+e.message)


def writeRiak(riak, action, bucketname, key, data, mimeType, index=None):
	bucket = riak.bucket(bucketname)
	if action == "write":
		obj = RiakObject(riak, bucket, key)
		obj.content_type = mimeType
		if mimeType == "application/octet-stream":
			obj.encoded_data = data
		else:
			obj.data = data
		if index is not None:
				for indexKey in index:
					try:
						obj.add_index(indexKey,index[indexKey])
					except Exception as e:
							logger.error("Error updating index: "+e.message)
		startTime = time.time()
		obj.store()
		duration = round((time.time() - startTime),3)
		logger.info(" Block write "+(bucketname[-3:])+"/"+key+" Sz: "+str(len(data))+" Dur: "+str(duration))
	elif action == "delete":
		got = bucket.get(key)
		startTime = time.time()
		got.delete()
		duration = round((time.time() - startTime),3)
		logger.info(" Block delete "+(bucketname[-3:])+"/"+key+" Duration: "+str(duration))

def modifyBlocks(riak, action, pid, startTime, endTime, interval,payload):
	logger.info("Adding blocks. Start: "+str(startTime)+" End: "+str(endTime)+" Int: "+str(interval)+" Pay: "+payload)
	if isinstance(payload, int):
		writePayload = open("/dev/urandom","rb").read(payload)		
		mimeType="application/octet-stream"
	elif isinstance(payload, basestring):
		try:
			f = open(payload, "rb")
			writePayload=f.read()
			mimeType="application/octet-stream"
		except:
			raise Exception, "Error reading block file "+payload
		finally:
			f.close()
	else:
		writePayload=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(int(payload)))
		mimeType='text/plain'
	for x in range(startTime, endTime, interval):
		index={"start_int":int(x+1),"end_int":int(x+interval)}
		writeRiak(riak, action, pid, str(x+1)+":"+str(x+interval), writePayload,mimeType,index)


def configRiak(clusterAddresses, clusterPort):
	nodes=[]
	for eachNode in clusterAddresses:
		thisNode= { 'host': eachNode, 'pb_port': clusterPort}
		nodes.append(thisNode)
	client = RiakClient(protocol='pbc',nodes=nodes)
	return client

def getCmdLineParser():
    """ Create a command line parser to provide additional program information

    :return:  instance of argparse.ArgumentParser

    """
    import argparse
    desc = 'Test coverage algorithm'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-f', '--file', default='coverageTestCases_config',
                        help='optional configuration file name (*.ini format)')

    return parser

def keysToArray(keys):
	tempArray=[]
	#logger.info("Keys: "+str(len(keys)))
	for key in keys:
		newArray=[int(x) for x in key.split(":")]
		tempArray.append(newArray)

	#logger.info("Array: "+str(len(tempArray)))
	return sorted(tempArray)

def dumpArray(theArray):
	s="[ "
	for interval in theArray:
		s=s+"["+str(interval[0])+", "+str(interval[1])+"], "
	s=s+" ]"

	return s

def calculateCoverage(riak, bucketName, startTime, endTime):

	coverageArray=[]
	bucket = riak.bucket(bucketName)

	#Check the start point...
	startArray=keysToArray(bucket.get_index('start_int', 0, startTime))
#	logger.info("len: "+str(len(startArray))+" Start: "+dumpArray(startArray))
#	logger.info("len: "+str(len(endArray))+" End: "+dumpArray(endArray))
	if len(startArray)>1:
		lastStart=startArray[len(startArray)-1]
		#logger.info("lastStart: ["+str(lastStart[0])+", "+str(lastStart[1])+"]")
	elif len(startArray)==1:	
		lastStart=startArray[1]
		#logger.info("lastStart: ["+str(lastStart[0])+", "+str(lastStart[1])+"]")

	if lastStart is not None:
		if startTime in lastStart:
			addOne=[]
			addOne.append(startTime)
			addOne.append(lastStart[1])
			coverageArray.append(addOne)

	#Get the middle...
	greaterThanStart=keysToArray(bucket.get_index('start_int', startTime,10000000000))
	lessThanEnd=keysToArray(bucket.get_index('end_int', 0,endTime))
	common = set(map(tuple, greaterThanStart)) & set(map(tuple, lessThanEnd))
	coverageArray.extend(sorted(common))

	#Check the end point...
	endArray=keysToArray(bucket.get_index('end_int', endTime, 10000000000))
	if len(endArray)>0:
		if endTime in endArray[0]:
			addOne=[]
			addOne.append(endArray[0][0])
			addOne.append(endTime)
			coverageArray.append(addOne)


	return coverageArray

	

if __name__ == '__main__':

    ##################################################
    # Get command line argument parser
    #
	p = getCmdLineParser()
	args = p.parse_args()

    ##################################################
    # Read configuration parameters from a file
    #
	cfg = RawConfigParser()
	cfg.read(args.file)

    ##################################################
    # Fire up the logger 
    #
	logger = initLog()
	logger.info('Starting Run: '+currentDayStr()+'  ==============================')


    ##################################################
    # Configure the connection to Riak 
    #
	riak = configRiak(json.loads(cfg.get('riak', 'cluster')),cfg.get('riak', 'port'))
	#modifyTestData(riak,"write",cfg.get('app', 'testfile'))
	#modifyTestData(riak,"delete",cfg.get('app', 'testfile'))

	startQTime = time.time()
	coverage=calculateCoverage(riak,'93AF8721-1676-44B3-02EB-F9916F7AC46F', 8400,8600)
	duration = round((time.time() - startQTime),3)
	
	logger.info("Duration: "+str(duration)+" Results: "+dumpArray(coverage))
