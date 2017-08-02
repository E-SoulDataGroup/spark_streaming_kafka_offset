package streamingtest

import java.sql.Time
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.json.JSONObject
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/13.
  */
object DataProcessor {

  private val countEventVega: mutable.HashMap[String, Int] = mutable.HashMap(("delta", 0), ("funnel", 0), ("change", 0), ("inventory", 0))
  val countEventAd: mutable.HashMap[String, mutable.HashMap[String, Any]] = mutable.HashMap()
  val idIP: mutable.HashMap[String, ArrayBuffer[String]] = mutable.HashMap()
  private implicit val formats = net.liftweb.json.DefaultFormats
  val SdfFilter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfFilterDate = new SimpleDateFormat("yyyy-MM-dd")

  def vegaDataProcessor(item: SparkFlumeEvent): String = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)
    val eventArray = jsonObj.getJSONArray("events")
    var resString: String = "real-time: "
    if (eventArray.length() >= 1) {
      for (i <- 1 to eventArray.length()) {
        val key = eventArray.getJSONObject(i - 1).getString("type")
        countEventVega(key) += 1
        resString = resString.concat(key + ": " + countEventVega(key).toString + "\t")
      }
    }
    compact(render(decompose(countEventVega.toMap)))
  }

  def adDataProcessor(item: SparkFlumeEvent): String = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)

    val adID = jsonObj.getInt("Adid").toString
    val adIP = jsonObj.getString("userip")
    val adDate = jsonObj.getString("Adatetime").split(" ")(0) + " 00:00:00"
    val currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    if (!idIP.contains(adID)) {
      idIP.put(adID, ArrayBuffer(adIP))
    }
    else {
      if (idIP(adID).indexOf(adIP) == -1) {
        idIP(adID) += adIP
      }
    }
    if (!countEventAd.contains(adID)) {
      countEventAd.put(adID, mutable.HashMap(("count", 1), ("ipcount", idIP(adID).length), ("addate", adDate), ("logTime", currentTime)))
    }
    else {
      countEventAd(adID)("count") = countEventAd(adID)("count").toString.toInt + 1
      countEventAd(adID)("ipcount") = idIP(adID).length
      countEventAd(adID)("addate") = adDate
      countEventAd(adID)("logTime") = currentTime
    }
    //注意： Map(1005 -> Map(count -> 1, ipcount -> 1, addate -> 2017-04-17 00:00:00)) 会被转化为
    // {"1005":[{"_1":"count","_2":1},{"_1":"ipcount","_2":1}, {"_1":"addate", "_2":"2017-04-17 00:00:00"}]}
    compact(render(decompose(countEventAd.toMap)))
  }

  def adDataProcessorPV(item: SparkFlumeEvent): Tuple2[String, Tuple2[Int, String]] = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)
    val adID = jsonObj.getInt("Adid").toString
    val adDate = jsonObj.getString("Adatetime").split(" ")(0)
    (adID, (1, adDate))
  }

  def adDataProcessorUV(item: SparkFlumeEvent): Tuple2[String, Tuple2[Array[String], String]] = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)
    val adID = jsonObj.getInt("Adid").toString
    val adDate = jsonObj.getString("Adatetime").split(" ")(0)
    val adIP = jsonObj.getString("userip")
    (adID, (Array(adIP), adDate))
  }

  def adDataProcessorPV2(item: SparkFlumeEvent): Tuple2[String, Tuple2[Int, String]] = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)
    val adID = jsonObj.getInt("channel").toString
    val adDate = jsonObj.getString("time").split(" ")(0)
    (adID, (1, adDate))
  }

  def adDataProcessorUV2(item: SparkFlumeEvent): Tuple2[String, Tuple2[Array[String], String]] = {
    val jsonStr = new String(item.event.getBody.array(), "utf-8")
    val jsonObj = new JSONObject(jsonStr)
    val adID = jsonObj.getInt("channel").toString
    val adDate = jsonObj.getString("time").split(" ")(0)
    val adIP = jsonObj.getString("uid")
    (adID, (Array(adIP), adDate))
  }

  def dataAccumulator(currValues:Seq[(String, Int, String, String)], preValue:Option[(String, Int, Array[String], String)]): Option[(String, Int, Array[String], String)] = {
    val preValueNew = preValue.getOrElse(("", 0, Array[String](), "1970-01-01"))
    if (currValues.nonEmpty) {
      val channelID = currValues.head._1
      var dateCurrent = new Date()
      var dateRecord = new Date()
      try {
        dateCurrent = sdfFilterDate.parse(currValues.head._4) // 数据中日期
        dateRecord = sdfFilterDate.parse(preValueNew._4) // 已存的日期
      }
      catch {
        case ex: Exception => println(ex + ": " + currValues.head._4 + " " + preValueNew._4)
      }
      if (dateCurrent.after(dateRecord)) {
        val PVSum = currValues.map(x => x._2).sum
        var UVArray:Array[String] = Array()
        currValues.foreach(x => UVArray :+= x._3)
        Some((channelID, PVSum, UVArray.distinct, currValues.head._4)) // 新日期，重新计算
      }
      else if (dateCurrent.equals(dateRecord)) {
        val PVSum = currValues.map(x => x._2).sum + preValueNew._2
        var UVArray = preValueNew._3
        currValues.foreach(x => UVArray :+= x._3)
        Some((channelID, PVSum, UVArray.distinct, currValues.head._4)) // 旧日期，累加
      }
      else {
        Some(preValueNew) // 过期日期，忽略
      }
    }
    else {
      Some(preValueNew)
    }
  }
}
