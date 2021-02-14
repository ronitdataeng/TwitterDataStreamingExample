from pyspark import *
from pyspark.sql import functions as sf, SparkSession
from pyspark.sql.types import Row
from pyspark.streaming import StreamingContext
from Config.ReadGlobalConfig import *
from cassandraData import cassandra_writedate


def savetheresult(rdd):
    if not rdd.isEmpty():
        rdd_mapped = rdd.map(lambda w: Row(value=w))
        spdf_streamdata = rdd_mapped.toDF()
        spdf_streamdata = spdf_streamdata.filter("Value is not null")
        spdf_datatowrite = spdf_streamdata.withColumn('Text', sf.split(spdf_streamdata['value'], '@@#').getItem(0)) \
            .withColumn('Name', sf.split(spdf_streamdata['value'], '@@#').getItem(1)) \
            .withColumn('UserName', sf.split(spdf_streamdata['value'], '@@#').getItem(2)) \
            .withColumn('UserID', sf.split(spdf_streamdata['value'], '@@#').getItem(3)) \
            .withColumn('TimeStamp', sf.split(spdf_streamdata['value'], '@@#').getItem(4)) \
            .drop('value') \
            .filter((sf.col('UserID').isNotNull()) & (sf.col('TimeStamp').isNotNull()))
        cassandra_writedate(spdf_datatowrite.toPandas())


if __name__ == "__main__":
    # create spark configuration
    sc = SparkContext()
    spark = SparkSession(sc)
    # create spark context with the above configuration
    ssc = StreamingContext(sc, 10)
    # read data from port
    dataStream = ssc.socketTextStream(host, twitterstreamingport).window(10)
    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split("@@#data_end@@#"))

    # do processing for each RDD generated in each interval
    words.foreachRDD(savetheresult)
    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
