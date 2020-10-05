import kafka

consumer = kafka.KafkaConsumer('log-entry', bootstrap_servers='ip-10-0-0-10.us-west-2.compute.internal:9092')

for msg in consumer:
    print(msg.value)
    pass

 