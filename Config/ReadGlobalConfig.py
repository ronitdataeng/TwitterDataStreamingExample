import json

f = open("Config/GlobalConfig.json")
configdata = json.load(f)
AccessToken = configdata['AccessToken']
AccessSecret = configdata['AccessSecret']
consumer_key = configdata['consumer_key']
consumer_secret = configdata['consumer_secret']
cassandraport = configdata['cassandraport']
twitterstreamingport = configdata['twitterstreamingport']
host = configdata['host']
