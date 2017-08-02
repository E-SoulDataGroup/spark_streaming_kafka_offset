package streamingtest

import net.liftweb.json._
import java.util.{Calendar, Date}
/**
  * Created by Administrator on 2017/4/14.
  */
object Utils {

  def clearRecord(): Unit = {
    val timer = new java.util.Timer()
    val startTime = getTomorrowZeroTime
    println("Clear interval start time: " + startTime.toString)
    val task = new java.util.TimerTask {
      def run(): Unit = {
        DataProcessor.countEventAd.clear()
        DataProcessor.idIP.clear()
      }
    }
    timer.schedule(task, startTime, ConfigurationConstants.clearInterval)
  }

  def getTomorrowZeroTime: Date = {
    var date = new Date()
    val day = Calendar.getInstance()
    day.setTime(date)
    val dayNum = day.get(Calendar.DATE)
    day.set(Calendar.DATE, dayNum + 1)
    day.set(Calendar.HOUR_OF_DAY, 0)
    day.set(Calendar.MINUTE, 0)
    day.set(Calendar.SECOND, 0)
    day.set(Calendar.MILLISECOND, 0)
    date = day.getTime
    date
  }

  def jsonStringToValueString(jsonStr: String): List[String] = {
    val json = parse(jsonStr)
    val resString = for {
      JField(id, JArray(content)) <- json
      JField("_2", JInt(count)) <- (json \ id)(0)
      JField("_2", JInt(ipcount)) <- (json \ id)(2)
      JField("_2", JString(addate)) <- (json \ id)(1)
      JField("_2", JString(logtime)) <- (json \ id)(3)
    } yield (id, "'" + addate + "'", count, ipcount, "'" + logtime + "'").toString()
    resString
  }

  def main (args: Array[String]): Unit = {
    val a = jsonStringToValueString("{\"1005\":[{\"_1\":\"count\",\"_2\":1},{\"_1\":\"addate\", \"_2\":\"2017-04-17 00:00:00\"},{\"_1\":\"ipcount\",\"_2\":1}], \"1001\":[{\"_1\":\"count\",\"_2\":2},{\"_1\":\"addate\", \"_2\":\"2017-04-17 00:00:00\"}, {\"_1\":\"ipcount\",\"_2\":1}]}")
    val res = a.toString().substring(0, a.toString().length-1).replace("List(", "")
    println(res)
  }
}
