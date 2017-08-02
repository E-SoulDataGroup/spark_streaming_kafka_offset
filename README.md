### spark_streaming_kafka_offset

* 在Spark Streaming中使用Kafka作为数据源
* 利用MySQL保存Kafka offset偏移量来记录消费的位置
* Spark程序重启后可以从上次消费位置继续消费，不会丢失数据
* 使用checkpoint保证状态结果不丢失
