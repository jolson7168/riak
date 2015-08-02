import uuid
import time
import datetime

#fixed to include offsets....
#and startdate....

def getTimeOffset(interval, units):

	if units=="minutes":
		retval=interval*60*1000
	elif units=="seconds":
		retval=interval*1000
	elif units=="hours":
		retval=interval*60*60*1000
	elif units=="days":
		retval=interval*24*60*60*1000

	return retval

def getEndTime(startTime, startTimeMask, duration, durationUnits):
	
	date_1 = datetime.datetime.strptime(startTime, startTimeMask)
	if durationUnits=="days":
		end_date = date_1 + datetime.timedelta(days=duration)
		unixTime=int(end_date.strftime('%s'))*1000
	return unixTime

def dateToString(intDate, timeMask, divisor):
	return datetime.datetime.fromtimestamp(intDate / divisor).strftime(timeMask)

def dumpIntervalStatus(monitorStatus, currentTime, timeMask, multiplier):
	if monitorStatus == True:
		print(dateToString(currentTime, timeMask, multiplier)+" / "+ str(currentTime) + " ON")
	elif monitorStatus == False:
		print(dateToString(currentTime, timeMask, multiplier)+" / "+ str(currentTime) + " OFF")

def calculatePayload(size, units):
	payloadSize = 0
	if units=="Mb":
		payloadSize=size*1024*1024
	return payloadSize

def getBoolean(booleanParam):
	if strictParam=="False":
		return False
	else:
		return True

def generateData(testParams):
	data=[]
	startTime = getTime(testParams["startTime"],testParams["startTimeMask"],testParams["multiplier"])
	interval = getTimeOffset(testParams["interval"],testParams["intervalUnits"])
	endTime = getEndTime(testParams["startTime"],testParams["startTimeMask"],testParams["duration"],testParams["durationUnits"])
	onTime = getTimeOffset(testParams["onTime"],testParams["onTimeUnits"]) 
	offTime = getTimeOffset(testParams["offTime"],testParams["offTimeUnits"])
	payload = calculatePayload(testParams["size"], testParams["sizeUnits"])
	strict = getBoolean(testParams["strict"])
	generateUUID = getBoolean(testParams["generateUUID"])
	currentTime=startTime
	newStartTime=startTime
	monitorStatus = True
	currentMonitorStatus = True
	offset=0
	if not strict:
		offset = offset+interval
	while currentTime<endTime:
		if ((offset>=onTime) and (monitorStatus)) or ((offset>=offTime) and (not monitorStatus)):
			offset=0
			if not strict:
				offset = offset+interval
			monitorStatus = not monitorStatus

		offset=offset+interval
		if monitorStatus != currentMonitorStatus:
			if not monitorStatus:
				if generateUUID:
					data.append(str(uuid.uuid1()).upper()+", "+str(newStartTime)+", "+str(currentTime)+", "+str(interval)+", "+str(int(payload)))
				else:
					data.append(str(newStartTime)+", "+str(currentTime)+", "+str(interval)+", "+str(int(payload)))

			if not strict:
				currentTime = currentTime +interval

			newStartTime = currentTime
			currentMonitorStatus=monitorStatus


		currentTime = currentTime+interval


	return data

def getTime(timeString, timeMask, multiplier):
	return int(time.mktime(datetime.datetime.strptime(timeString, timeMask).timetuple())*multiplier)

def generateRandomBytes(size):
	return open("/dev/urandom","rb").read(size)

def saveBytes(fname, bytes):
	target = open(fname, 'wb')
	target.write(bytes)
	target.close

def writeFiles(testParams, data):
	f=open(testParams["testfilename"],'w')
	f.write("# "+testParams["testfilename"]+"\n")
	f.write("# "+testParams["comment"]+"\n")
	for block in data:
		f.write(block+"\n")
	f.close()
 

testDataInputs={"onTime":96, "onTimeUnits":"hours",
				"offTime":24, "offTimeUnits":"hours",
				"size":1.2, "sizeUnits":"Mb",
				"interval":10,"intervalUnits":"minutes", 
				"duration":365,"durationUnits":"days", 
				"strict":"False",
				"generateUUID":"False",
				"startTime":"2015-01-01 120000.000", "startTimeMask":"%Y-%m-%d %H%M%S.%f", "multiplier":1000,
				"testfilename":"profile5.csv", "comment":"4 days on, 1 day off starting midnight 01 Jan 2015, for one year, 1.2mb/10min"}   



testData = generateData(testDataInputs)
writeFiles(testDataInputs, testData)




