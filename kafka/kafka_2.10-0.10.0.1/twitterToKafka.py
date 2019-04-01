from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler


consumer_key =  'EL9bP53IKTBWsmNVkXeBaTLH0'
consumer_secret = 'URuSzNfTCFBq7nPyaSqAjqksECbD2vUFUMq2RfhYSjoloWPQbq'
access_token = '1063944351724662784-MVTRTpJk2JdoFTOl3UeVt04orBjSUJ'
access_token_secret =  'GgwSxgkQVrXfnsQXxbuon3CDmV9GERKz0H8EYfv3Db76p'


class KafkaListener(StreamListener):
    def on_data(self, streamData):
        producer.send_messages("twitterStream", streamData.encode('utf-8'))
        print (streamData)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
