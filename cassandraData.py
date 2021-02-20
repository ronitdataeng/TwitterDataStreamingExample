from cassandra.cluster import Cluster
from Config.ReadGlobalConfig import *

cluster = Cluster([host], port=cassandraport)
session = cluster.connect()


def cassandra_writedate(df):
    """

    """

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
    , UserID text
    , TimeStamp text
    , Date text
    , Hour int
    , PRIMARY KEY (UserID,TimeStamp,Date))""")

    query_insert = "INSERT INTO twitterusertable " \
                   "(Text, Name, UserName, UserID,TimeStamp,Date,Hour) " \
                   "VALUES (?, ?, ?, ?, ?, ?, ?)"
    prepared = session.prepare(query_insert)
    for index, row in df.iterrows():
        session.execute(prepared
                        , (row['Text']
                           , row['Name']
                           , row['UserName']
                           , row['UserID']
                           , row['TimeStamp']
                           , row['Date']
                           , row['Hour']))
    return session.set_keyspace('twitterdata')


def cassandra_getdate(date):
    """

    """
    session.set_keyspace('twitterdata')
    rowdata = session.execute(
        "SELECT * FROM twitterusertable where date=" + "'" + date + "'" + " ALLOW FILTERING")
    return rowdata.current_rows
