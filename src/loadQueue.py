import json
import csv
import pika
import sys
import getopt

def sendData(uploadFile,queueserver,login, password, exchange, route):

	credentials = pika.PlainCredentials(login, password)
	connection = pika.BlockingConnection(pika.ConnectionParameters(queueserver,credentials=credentials))
	channel = connection.channel()

	try:
		ifile  = open(uploadFile, "rb")
		reader = csv.reader(ifile)
		for row in reader:
			if not row[0][0]=="#":
					msg={"action":"write","id":row[0],"startTime":int(row[1]),"endTime":int(row[2]),"interval":int(row[3]),"size":int(row[4])}
					channel.basic_publish(exchange=exchange,routing_key=route,body=json.dumps(msg))
					print "Sent: "+json.dumps(msg)
	except Exception as e:
		print ("Error loading test data: "+e.message)

	connection.close()


def main(argv):
 	try:
		opts, args = getopt.getopt(argv,"hdqlper:",["datafile=","queueserver=","login=","password=","exchange=","route="])
	except getopt.GetoptError:
		print ('loadQueue.py -d <file to upload> -q <ip of queue server> -l <login> -p <password> -e <exchange> -r <route>')
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print ('loadQueue.py -d <file to upload> -q <ip of queue server> -l <login> -p <password> -e <exchange> -r <route>')
			sys.exit()
		elif opt in ("-d", "--datafile"):
			uploadFile=arg
			try:
				with open(uploadFile): pass
			except IOError:
				print ('Upload file: '+uploadFile+' not found')
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

	sendData(uploadFile,queueserver,login, password, exchange, route)

if __name__ == "__main__":
	main(sys.argv[1:])