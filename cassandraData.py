from cassandra.cluster import Cluster
from Config.ReadGlobalConfig import *

def cassandra_writedate(df):
    """

    """
    cluster = Cluster([host], port=cassandraport)
    session = cluster.connect()
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS twitterdata
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """)

    session.set_keyspace('twitterdata')
    session.execute("""CREATE TABLE IF NOT EXISTS twitterusertable (
    Text text
    , Name text
    , UserName text
    ,UserID text
    , TimeStamp text
    , PRIMARY KEY (UserID,TimeStamp))""")

    query_insert = "INSERT INTO twitterusertable " \
                   "(Text, Name, UserName, UserID,TimeStamp) " \
                   "VALUES (?, ?, ?, ?, ?)"
    prepared = session.prepare(query_insert)
    for index, row in df.iterrows():
        session.execute(prepared
                        , (row['Text']
                           , row['Name']
                           , row['UserName']
                           , row['UserID']
                           , row['TimeStamp']))
    return session.set_keyspace('twitterdata')

