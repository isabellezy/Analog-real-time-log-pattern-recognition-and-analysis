#!/usr/bin/env python3
import boto3
import codecs
import kafka
import json
import time
import sys
from datetime import datetime
import configparser
#import logging

config = configparser.ConfigParser()
config.read(sys.argv[1])
aws_key_id = config['myconfig']['aws_access_key_id']
aws_secret_key = config['myconfig']['aws_secret_access_key']
bootstrap_server = config['myconfig']['bootstrap_servers']
bucket = config['myconfig']['bucket']

i = 0
#logger = logging.getLogger('testdata')
s3 = boto3.client('s3', aws_access_key_id= aws_key_id, aws_secret_access_key= aws_secret_key) 

producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_server)
while True:
	obj = s3.get_object(Bucket=bucket, Key=sys.argv[1])
	body = obj['Body']
	for ln in codecs.getreader('utf-8')(body):
		producer.send('log-entry', 
		    json.dumps({"timestamp": time.time(),
		                "log": ln.strip()}).encode('utf-8'))
		    # json.dumps({"timestamp": time.time(),
		    #             "log": "dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.73.220:50010 is added to blk_7128370237687728475 size 67108864"}).encode('utf-8'))
		i += 1
		if i % 1000 == 0:
			print(f'{datetime.now()} Processed {i} messages')

	#time.sleep(2);
	



	


