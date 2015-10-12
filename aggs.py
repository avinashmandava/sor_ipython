import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def parse_message(message):
    row=message.split(',')

def main():
#main function to execute code
    sc = SparkContext(appName="CouponCounterPySpark")
    ssc = StreamingContext(sc,10)
    zk_host = "localhost:2181"
    consumer_group = "coupon-event-consumers"
    kafka_partitions={"test":1}
    #create kafka stream
    lines = KafkaUtils.createStream(ssc,zk_host,consumer_group,kafka_partitions)
    events = lines.map(lambda line: line[1].split(','))
    tmpagg = events.map(lambda event: ((event[0],event[1]),1) )
    coupon_counts = tmpagg.reduceByKey(lambda x,y: x+y)
    coupon_counts.pprint()
    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":
    main()
