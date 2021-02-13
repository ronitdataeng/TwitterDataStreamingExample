from cassandra.cluster import Cluster


def cassandra_getkeyspace():
    """

    """
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS twitterdata
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """)

    return session.set_keyspace('twitterdata')



