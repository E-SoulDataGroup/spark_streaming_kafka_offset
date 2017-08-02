package streamingtest.kafkaoffsethandler

import java.sql.PreparedStatement

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import streamingtest.{ConfigurationConstants, ConnectPool}
import streamingtest.FlumePollingEvent.logger


object OffsetReadAndSave {
  // 从mysql中读取kafka偏移量，并产生kafka DStream
  def KafkaOffsetRead(ssc: StreamingContext, kafkaParams: Map[String, String], consumerTopics: Set[String]): InputDStream[(String, String)] = {
    val connOffset = ConnectPool.getConnection
    val psOffsetCnt: PreparedStatement = connOffset.prepareStatement("SELECT SUM(1) FROM `kafka_offset` WHERE `topic`=?")
    psOffsetCnt.setString(1, ConfigurationConstants.kafkaConsumerTopics)
    val rs = psOffsetCnt.executeQuery()
    var parCount = 0
    while (rs.next()) {
      parCount = rs.getInt(1)
      println(parCount.toString)
    }
    var kafkaStream : InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val psOffsetRead: PreparedStatement = connOffset.prepareStatement("SELECT offset FROM `kafka_offset` WHERE `topic`=? AND `partition`=?")
    if (parCount > 0) {
      for (i <- 0 until parCount) {
        psOffsetRead.setString(1, ConfigurationConstants.kafkaConsumerTopics)
        psOffsetRead.setInt(2, i)
        val rs1 = psOffsetRead.executeQuery()
        while (rs1.next()) {
          val partitionOffset = rs1.getInt(1)
          val tp = TopicAndPartition(ConfigurationConstants.kafkaConsumerTopics, i)
          fromOffsets += (tp -> partitionOffset.toLong)
        }
      }
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, consumerTopics)
    }
    ConnectPool.closeCon(psOffsetCnt,connOffset)
    ConnectPool.closeCon(psOffsetRead,connOffset)
    kafkaStream
  }

  // 将kafka偏移量写入mysql中保存
  def KafkaOffsetSave(offsetRanges: Array[OffsetRange]): Unit = {
    val connOffset = ConnectPool.getConnection
    connOffset.setAutoCommit(false)
    val psOffset: PreparedStatement = connOffset.prepareStatement("REPLACE INTO `kafka_offset` (`topic`, `partition`, `offset`) VALUES (?,?,?)")
    for (o <- offsetRanges) {
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      psOffset.setString(1, o.topic.toString)
      psOffset.setInt(2, o.partition.toInt)
      psOffset.setLong(3, o.fromOffset.toLong)
      psOffset.addBatch()
    }
    psOffset.executeBatch()
    connOffset.commit()
    ConnectPool.closeCon(psOffset,connOffset)
  }

}
