import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
from datetime import datetime, date
from Config.ReadGlobalConfig import *


class Listener(StreamListener):

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
                      msg['user']['screen_name'] + "@@#" + msg['id_str'] + "@@#" +
                      str(datetime.today()) + "@@#data_end@@#"))

            # add at the end of each tweet "@@#data_end@@#" also adding '@@#' after each data object in a tweet
            self.client_socket \
                .send(str(msg['text'] + "@@#" + msg['user']['name'] + "@@#" +
                          msg['user']['screen_name'] + "@@#" + msg['id_str'] + "@@#" +
                          str(datetime.today()) + "@@#data_end@@#") \
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
    twitter_stream = Stream(auth, Listener(c_socket))
    twitter_stream.filter(track=keyword, languages=["en"])


if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    s.bind((host, twitterstreamingport))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword 'covid' for the tweet data
    senddata(c_socket, keyword=['covid'])
