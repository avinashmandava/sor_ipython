import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datetime

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
    tmpagg = events.map(lambda event: ((event[1]),1) )
    coupon_counts = tmpagg.reduceByKey(lambda x,y: x+y)
    coupon_records = coupon_counts.map(lambda x: {"offer_id" : x[0], "bucket" : str(datetime.datetime.now().strftime("%s")), "count" : int(x[1])})
    #coupon_records.pprint()
    #coupon_records.registerTempTable("coupon_counters")
    #coupon_records.select("offer_id","bucket","count").show()
    #coupon_records = coupon_counts.map(lambda record: {"offer_id" : record[0],"bucket" : str(int(datetime.datetime.now().strftime("%s"))*1000),"count" : int(record[1])}
    coupon_records.pprint()
    coupon_records.foreachRDD(lambda rdd: rdd.saveToCassandra("loyalty","coupon_counters"))
    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":
    main()
