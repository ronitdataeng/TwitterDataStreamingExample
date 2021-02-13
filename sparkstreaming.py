from pyspark.sql import functions as sf
from pyspark.streaming import StreamingContext
from pyspark import *


def savetheresult(rdd):
    if not rdd.isEmpty():
        rdd_mapped = rdd.map(lambda w: Row(value=w))
        df = rdd_mapped.toDF()
        df = df.filter("Value is not null")
        df1 = df.withColumn('Text', sf.split(df['value'], '@@#').getItem(0)) \
            .withColumn('Name', sf.split(df['value'], '@@#').getItem(1)) \
            .withColumn('UserName', sf.split(df['value'], '@@#').getItem(2)) \
            .drop('value')
        df1.show()


if __name__ == "__main__":
    # create spark configuration
    sc = SparkContext()
    # create spark context with the above configuration
    ssc = StreamingContext(sc, 10)
    # read data from port
    dataStream = ssc.socketTextStream("127.0.0.1", 9726).window(10)
    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split("@@#data_end@@#"))

    # do processing for each RDD generated in each interval
    words.foreachRDD(savetheresult)
    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
