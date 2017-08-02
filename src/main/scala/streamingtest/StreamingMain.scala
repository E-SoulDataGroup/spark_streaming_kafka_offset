package streamingtest

import java.sql.{PreparedStatement, ResultSet, Time}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{kafka, _}
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import org.slf4j.LoggerFactory

import streamingtest.kafkaoffsethandler.OffsetReadAndSave._


object StreamingMain extends Serializable {

  val SdfFilter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfFilterDate = new SimpleDateFormat("yyyy-MM-dd")
  val logger = LoggerFactory.getLogger(this.getClass)

  def dataProcessor(item: SparkFlumeEvent): String = {
    //dataContext.vegaDataProcessor(item)
    DataProcessor.adDataProcessor(item)
  }

  def main(args: Array[String]) {

    Utils.clearRecord()
    val host = ConfigurationConstants.sparkSinkHost
    val port = ConfigurationConstants.sparkSinkPort

    def functionToCreateContext(): StreamingContext = {

      val sparkConf = new SparkConf().setAppName(ConfigurationConstants.streamingAppName)
        .setMaster(ConfigurationConstants.deployMode)
        .set("spark.streaming.receiver.writeAheadLog.enable","true")
      sparkConf.set("spark.streaming.concurrentJobs", "5")
      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, ConfigurationConstants.streamingInterval)

      val storageLevel = ConfigurationConstants.streamingStorageLevel

      val consumerTopics = ConfigurationConstants.kafkaConsumerTopics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> ConfigurationConstants.kafkaBrokers)
      val kafkaStream = KafkaOffsetRead(ssc, kafkaParams, consumerTopics) // 从数据库中读取kafka偏移量并产生stream
      //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, consumerTopics)
      var offsetRanges = Array[OffsetRange]()

      kafkaStream.transform{ rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }.count().map(cnt => "Stream1 (ad_click): " + cnt + " Events received. time: " + new Date().getTime).print()
      // (ad_id, (ad_id, pv, uv, date))
      val streamAccumulatorAd = kafkaStream.map(item => {
        val jsonObj = new JSONObject(item._2)
        val dataTime = jsonObj.getString("Adatetime")
        (jsonObj.get("Adid").toString, (jsonObj.get("Adid").toString, 1, jsonObj.getString("userip"), dataTime))
      }).updateStateByKey(DataProcessor.dataAccumulator).mapValues(item => (item._1, item._2, item._3.length, item._4))

      var dfAd = SQLContext.getOrCreate(ssc.sparkContext).createDataFrame(sc.emptyRDD[Row], ConfigurationConstants.middleTableSchema1)

      val sql = "REPLACE INTO ad_h5game_realtime (short_id, channel_id, origin_game_id, pv, uv, ad_log_time) VALUES (?,?,?,?,?,?)"

      streamAccumulatorAd.foreachRDD( rdd => {
        val rddRow = rdd.values.map(item => Row.fromTuple(item))
        dfAd = SQLContext.getOrCreate(rdd.sparkContext).createDataFrame(rddRow, ConfigurationConstants.middleTableSchema1)
      })

      streamAccumulatorAd.foreachRDD( rdd => {
        val dfBase = SQLContext.getOrCreate(rdd.sparkContext).read.format("jdbc").options(ConfigurationConstants.connectOptions).load()
        // 注意每一次join之后生成新的中间表，每个字段的编号都会增加，后面再用之前表的字段会报错
        val res = dfAd.join(dfBase, dfAd("ad_id") === dfBase("short_id"), "inner")
          .selectExpr("ad_id as short_id", "channel as channel_id", "game_id as origin_game_id", "pv", "uv", "current_time as ad_log_time")
          .where("to_date(ad_log_time)=to_date(from_unixtime(unix_timestamp()))")

        res.show()
        KafkaOffsetSave(offsetRanges) // 将处理完的偏移量保存到数据库
      })
      ssc.checkpoint(ConfigurationConstants.checkpointDirectory)
      ssc
    }

    val context = StreamingContext.getOrCreate(ConfigurationConstants.checkpointDirectory, functionToCreateContext)

    sys.addShutdownHook({
      context.stop(stopSparkContext = true, stopGracefully = true)
    })
    context.start()
    context.awaitTermination()
  }
}
