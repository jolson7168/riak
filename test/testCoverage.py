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
					modifyBlocks(riak, action, int(row[0]),int(row[1]),int(row[2]),row[3])
	except Exception as e:
		logger.error("Error loading test data: "+e.message)


def writeRiak(riak, action, bucketname, key, data, mimeType):
	bucket = riak.bucket(bucketname)
	if action == "write":
		obj = RiakObject(riak, bucket, key)
		obj.content_type = mimeType
		if mimeType == "application/octet-stream":
			obj.encoded_data = data
		else:
			obj.data = data
		startTime = time.time()
		obj.store()
		duration = round((time.time() - startTime),3)
		logger.info(" Block write "+bucketname+"/"+key+" Sz: "+str(len(data))+" Dur: "+str(duration))
	elif action == "delete":
		got = bucket.get(key)
		startTime = time.time()
		got.delete()
		duration = round((time.time() - startTime),3)
		logger.info(" Block delete "+bucketname+"/"+key+" Duration: "+str(duration))

def modifyBlocks(riak, action, startTime, endTime, interval,payload):
	logger.info("Adding blocks. Start: "+str(startTime)+" End: "+str(endTime)+" Int: "+str(interval)+" Pay: "+payload)
	if isinstance(payload, basestring):
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
		writeRiak(riak, action, "coverageTest", str(x), writePayload,mimeType)


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
	modifyTestData(riak,"write",cfg.get('app', 'testfile'))
	modifyTestData(riak,"delete",cfg.get('app', 'testfile'))

#Notes
#curl http://104.197.19.1:8098/buckets/coverageTest/keys/20

