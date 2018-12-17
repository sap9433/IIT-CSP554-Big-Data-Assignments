from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='sandbox.hortonworks.com:6667',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
consumer.subscribe(['kptest1'])

for message in consumer:
  print(message)
