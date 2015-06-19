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

from ConfigParser import RawConfigParser

###################################################
# Third party modules (libraries)
#
import pika               	# RabbitMQ AMQP client
from riak import RiakClient
from riak import RiakObject
###################################################
# File global variables
#
version_info = ('0', '5', '0')     # Major, minor, patch
g_app_name = 'LoadRiakFromQ'
g_app_version = '.'.join(version_info)

g_end_prev = time.time()           # end time previous time


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

def configureRiak(riakIPs, riakPort):


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


def getCmdLineParser():
    import argparse
    desc = 'Pull data from rabbitMQ and put it on Riak'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-f', '--file', default='defragger',
                        help='optional configuration file name (*.ini format)')

    return parser

def writeRiak(riak, obj):
    print("Write!")

if __name__ == '__main__':

    p = getCmdLineParser()
    args = p.parse_args()

    cfg = RawConfigParser()
    cfg.read(args.file)
 
    logger = initLog()
    logger.info('Starting Run: '+currentDayStr()+'  ==============================')

    msgQServerURL = cfg.get('rabbitmq', 'server_url')
    msgQName = cfg.get('rabbitmq', 'queue')
    msgQLogin = cfg.get('rabbitmq', 'login')    
    msgQPass = cfg.get('rabbitmq', 'password')



    riakIP = cfg.get('riak', 'cluster')
    riakPort = cfg.get('riak', 'port')
    riak = configureRiak(riakIP, riakPort)

    credentials = pika.PlainCredentials(msgQLogin, msgQPass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(msgQServerURL,credentials=credentials))
    channel = connection.channel()

    done = False
    while not done:
        startTime = time.time()
        method_frame, header_frame, body = channel.basic_get(msgQName)
        duration = round((time.time() - startTime),3)
        logger.info("Fetched from LoadQ: "+str(duration))
        if method_frame:
            writeRiak(riak,json.loads(body))
            channel.basic_ack(method_frame.delivery_tag)
        else:
            done = True
    
    connection.close()   
    logger.info('Ending Run: '+currentDayStr()+'  ==============================')




