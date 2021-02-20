# TwitterDataStreamingExample
Twitter streaming data example, receiving and saving the data in Cassandra using Spark Streaming.  

First Run the below comment to spin up the Cassandra DB in docker:
docker run --name cassandratest -p 127.0.0.1:9042:9042 -e CASSANDRA_CLUSTER_NAME=MyCluster -e 
CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -e CASSANDRA_DC=datacenter1 -d cassandra

First run the twitterinputstream.py to strat the twitter data streaming.
Then run the sparkstreaming.py to save the streaming data in Cassandra DB.
