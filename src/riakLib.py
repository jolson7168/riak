
import csv
import json
import logging
import time

from riak import RiakClient
from riak import RiakObject


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
		indexTime=""
		if index is not None:
				for indexKey in index:
					try:
						startTime2 = time.time()
						obj.add_index(indexKey,index[indexKey])
						duration2 = round((time.time() - startTime2),3)
						indexTime = " - "+str(duration2)
					except Exception as e:
							logger.error("Error updating index: "+e.message)
		startTime = time.time()
		obj.store()
		duration = round((time.time() - startTime),3)
		logger.info(" Write "+(bucketname[-3:])+"/"+key+" Sz: "+str(len(data))+" Dur: "+str(duration) + indexTime)
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
