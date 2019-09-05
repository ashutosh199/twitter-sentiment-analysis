import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json
consumer_key = 'c6h0UGadMyGiVw59UK1ajZ6V7'
consumer_secret = 'W2fVHmzhEiN8ylKWp8MCEdg4f6wpq74Vpts7ytlFY6o0nOapZF'
access_token='854302757636890625-QPgJOe1g6OC5OWrVtywlN5Z4CSZa6y9'
access_token_secret = 'VsjBcXWgF9MbkOQJVtzuZl1qF1haPH3LXFE2RAfLM1tSf'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
#api = tweepy.API(auth)
class StdOutListener(StreamListener):
    def on_data(self, data):
	data=json.loads(data)
        try:
	   producer.send_messages("twitter", data["text"].encode('utf-8','ignore'))
	   print (data["text"])
	except:
	   print(data["text"])
	return True
kafka = KafkaClient("localhost:9092")
producer=SimpleProducer(kafka)
l=StdOutListener()
stream=Stream(auth, l)
stream.filter(track=['Dhoni', 'modi'], stall_warnings=True, languages = ['en'])
