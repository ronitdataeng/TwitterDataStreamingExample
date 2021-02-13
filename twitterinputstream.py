import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json

AccessToken = '1356926758826704897-ocfGahmGZrXNTS9GynKHbc3bDD9WA9'
AccessSecret = 'A2BgoiIi8ni85yFpNSoZRuccvXQ5E9CYo4Ni3mVdA8KLU'
consumer_key = 'YJLhbqwl4tS8im6moFQuzczxU'
consumer_secret = 'A7CrsTFwTFrfoz8foadTMQt1Enc8nNtpgweiFY9ISQV4oQ3fhP'


class listener(StreamListener):

    # tweet object listens for the tweets
    def __init__(self, csocket):
        super().__init__()
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print("new message")
            print(msg)
            print(str(msg['text'] + "@@#" + msg['user']['name'] + "@@#" +
                      msg['user']['screen_name'] + "@@#" + "data_end"))
            # add at the end of each tweet "t_end"

            self.client_socket \
                .send(str(msg['text'] + "@@#" + msg['user']['name'] + "@@#" +
                          msg['user']['screen_name'] + "@@#" + "@@#data_end@@#") \
                      .encode('utf-8'))
            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def senddata(c_socket, keyword):
    print('start sending data from Twitter to socket')
    # authentication based on the credentials
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(AccessToken, AccessSecret)
    # start sending data from the Streaming API
    twitter_stream = Stream(auth, listener(c_socket))
    twitter_stream.filter(track=keyword, languages=["en"])


if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "127.0.0.1"
    port = 9726
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    senddata(c_socket, keyword=['covid'])
