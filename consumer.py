from textblob import TextBlob
from kafka import KafkaConsumer
consumer = KafkaConsumer('twitter',
                          bootstrap_servers=['localhost:9092'])
for message in consumer:
	print(message.value.decode('utf-8'))
	analysis = TextBlob(message.value.decode('utf-8'))
	print(analysis.sentiment)
	print("")


