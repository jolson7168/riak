
import csv
import json
import logging
import time

from riak import RiakClient
from riak import RiakObject


def keysToArray(keys):
	tempArray=[]
	#logger.info("Keys: "+str(len(keys)))
	for key in keys:
		newArray=[int(x) for x in key.split(":")]
		tempArray.append(newArray)

	#logger.info("Array: "+str(len(tempArray)))
	return sorted(tempArray)

def testIfInt(candidate):
	retval = False
	try:
		intPayload = int(candidate)
		retval = True
	except ValueError:
		pass  
	return retval

def writeRiak(riak, action, bucketname, key, data, mimeType, logger, index=None):
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
		storedObj = obj.store()
		duration = round((time.time() - startTime),3)
		if storedObj is not None:
			results = storedObj.key
		else:
			results = "Not stored!"
		logger.info(" Write "+(bucketname[-3:])+"/"+key+" Sz: "+str(len(data))+" Dur: "+str(duration)+" Results: "+results)
	elif action == "delete":
		got = bucket.get(key)
		startTime = time.time()
		got.delete()
		duration = round((time.time() - startTime),3)
		logger.info(" Block delete "+(bucketname[-3:])+"/"+key+" Duration: "+str(duration))

def modifyBlocks(riak, action, pid, startTime, endTime, interval,payload,logger):
	logger.info("Adding blocks. Start: "+str(startTime)+" End: "+str(endTime)+" Int: "+str(interval)+" Pay: "+str(payload))
	isInt=testIfInt(payload)
	if isInt:
		writePayload = open("/dev/urandom","rb").read(int(payload))		
		mimeType="application/octet-stream"
	else:
		try:
			f = open(payload, "rb")
			writePayload=f.read()
			mimeType="application/octet-stream"
		except:
			raise Exception, "Error reading block file "+payload
		finally:
			f.close()
	#else:
	#	writePayload=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(int(payload)))
	#	mimeType='text/plain'
	for x in range(startTime, endTime, interval):
		index={"start_int":int(x),"end_int":int(x+interval)}
		writeRiak(riak, action, pid, str(x)+":"+str(x+interval), writePayload,mimeType,logger, index)

def cleanupArray(coverage,gap):
	retval = []
	currentStart = 0
	x=0 
	while x < (len(coverage)-1):
		if currentStart == 0:
			currentStart = coverage[x][0]
		if ((coverage[x+1][0] - coverage[x][1]) > gap):
			currentEnd = coverage[x][1]
			newOne=[]
			newOne.append(currentStart)
			newOne.append(currentEnd)
			retval.append(newOne)
			currentStart=coverage[x+1][0]
		x=x+1
	if currentStart>0:
			newOne=[]
			newOne.append(currentStart)
			newOne.append(coverage[x][1])
			retval.append(newOne)

	return retval

def calculateCoverage(riak, bucketName, startTime, endTime, gap):

	coverageArray=[]
	retval = []
	bucket = riak.bucket(bucketName)

	lastStart=None
	startArray=keysToArray(bucket.get_index('start_int', 0, startTime))
	if len(startArray)>1:
		lastStart=startArray[len(startArray)-1]
	elif len(startArray)==1:	
		lastStart=startArray[0]  #1?

	if lastStart is not None:
		if startTime in lastStart:
			addOne=[]
			addOne.append(startTime)
			addOne.append(lastStart[1])
			coverageArray.append(addOne)

	#Get the middle...
	greaterThanStart=keysToArray(bucket.get_index('start_int', startTime,10000000000000))
	lessThanEnd=keysToArray(bucket.get_index('end_int', 0,endTime))
	common = set(map(tuple, greaterThanStart)) & set(map(tuple, lessThanEnd))
	coverageArray.extend(sorted(common))

	#Check the end point...
	endArray=keysToArray(bucket.get_index('end_int', endTime, 10000000000000))
	if len(endArray)>0:
		if endTime in endArray[0]:
			addOne=[]
			addOne.append(endArray[0][0])
			addOne.append(endTime)
			coverageArray.append(addOne)

	retval = cleanupArray(coverageArray,gap)
	return retval


def calculateCoverage2(riak, bucketName, startTime, endTime, gap):

	coverageArray=[]
	retval = []
	bucket = riak.bucket(bucketName)

	lastStart=None
	startArray=keysToArray(bucket.get_index('start_int', 0, startTime))
	if len(startArray)>1:
		lastStart=startArray[len(startArray)-1]
	elif len(startArray)==1:	
		lastStart=startArray[0]  #1?

	if lastStart is not None:
		if startTime in lastStart:
			addOne=[]
			addOne.append(startTime)
			addOne.append(lastStart[1])
			coverageArray.append(addOne)

	#Get the middle...
	greaterThanStart=keysToArray(bucket.get_index('start_int', startTime,endTime))
	lessThanEnd=keysToArray(bucket.get_index('end_int', startTime,endTime))
	common = set(map(tuple, greaterThanStart)) & set(map(tuple, lessThanEnd))
	coverageArray.extend(sorted(common))

	#Check the end point...
	endArray=keysToArray(bucket.get_index('end_int', endTime, endTime+600000))
	if len(endArray)>0:
		if endTime in endArray[0]:
			addOne=[]
			addOne.append(endArray[0][0])
			addOne.append(endTime)
			coverageArray.append(addOne)

	retval = cleanupArray(coverageArray,gap)
	return retval


def configureRiak(riakIPs, riakPort,logger):


    initial_attempts = 5
    num_attempts = 0

    initial_delay = 5      # 5 seconds between between initial attempts
    longer_delay = 5*60    # 5 minutes = 300 seconds
    delay_time = initial_delay

    nodes=[]
    for eachNode in riakIPs.split(","):
        thisNode= { 'host': eachNode, 'pb_port': riakPort}
        nodes.append(thisNode)

    riakIP=""
    for node in nodes:
        riakIP=json.dumps(node)+ " - " +riakIP

    logger.info('[STATE] Connecting to Riak...')

    connected = False  
    client = RiakClient(protocol='pbc',nodes=nodes)
    while not connected:
	   try:
            logger.info("Attempting to PING....")
            client.ping()
            connected = True
	   except:
            num_attempts += 1
            logger.error('EXCP: No Riak server found')
            if num_attempts == initial_attempts:
                delay_time = longer_delay
                # Wait to issue next connection attempt
                time.sleep(delay_time)

    logger.info('[STATE] Connected to Riak. Successful PING')
    return client
