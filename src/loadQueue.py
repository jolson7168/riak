import json
import csv
import pika
import sys
import getopt

def sendData(uploadFile, queueserver, login, password, exchange, route, action, uuid):

	credentials = pika.PlainCredentials(login, password)
	connection = pika.BlockingConnection(pika.ConnectionParameters(queueserver,credentials=credentials))
	channel = connection.channel()

	try:
		ifile  = open(uploadFile, "rb")
		reader = csv.reader(ifile)
		for row in reader:
			if not row[0][0]=="#":
					if uuid is None:
						msg={"action":action,"id":row[0],"startTime":int(row[1]),"endTime":int(row[2]),"interval":int(row[3]),"size":int(row[4])}
					else:
						msg={"action":action,"id":uuid,"startTime":int(row[0]),"endTime":int(row[1]),"interval":int(row[2]),"size":int(row[3])}
					channel.basic_publish(exchange=exchange,routing_key=route,body=json.dumps(msg))
					print "Sent: "+json.dumps(msg)
	except Exception as e:
		print ("Error loading queue data: "+e.message)

	connection.close()


def main(argv):
	mode = "datafile"
 	try:
		opts, args = getopt.getopt(argv,"hmdqlpera:",["mapfile=","datafile=","queueserver=","login=","password=","exchange=","route=","action="])
	except getopt.GetoptError:
		print ('loadQueue.py -d <file to upload> -q <ip of queue server> -l <login> -p <password> -e <exchange> -r <route> -a <action>')
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print ('loadQueue.py -d <file to upload> -q <ip of queue server> -l <login> -p <password> -e <exchange> -r <route> -a <action>')
			sys.exit()
		elif opt in ("-d", "--datafile"):
			uploadFile=arg
			try:
				with open(uploadFile): pass
			except IOError:
				print ('Upload file: '+uploadFile+' not found')
				sys.exit(2)
		elif opt in ("-m", "--mapfile"):
			mode="mapfile"
			uploadFile=arg
			try:
				with open(uploadFile): pass
			except IOError:
				print ('map file: '+uploadFile+' not found')
				sys.exit(2)
		elif opt in ("-q", "--queueserver"):
			queueserver=arg
		elif opt in ("-l", "--login"):
			login=arg
		elif opt in ("-p", "--password"):
			password=arg
		elif opt in ("-e", "--exchange"):
			exchange=arg
		elif opt in ("-r", "--route"):
			route=arg
		elif opt in ("-a", "--action"):
			action=arg

	if mode == "datafile":
		sendData(uploadFile, queueserver, login, password, exchange, route, action)
	else:
		try:
			ifile  = open(uploadFile, "rb")
			reader = csv.reader(ifile)
			for row in reader:
				if not row[0][0]=="#":
					sendData(row[1], queueserver, login, password, exchange, route, action,row[0])	
		except Exception as e:
			print ("Error loading map data: "+e.message)

if __name__ == "__main__":
	main(sys.argv[1:])