package com.flink.logcenter.task

import cn.hutool.core.util.StrUtil
import cn.hutool.json.{JSONObject, JSONUtil}
import com.flink.logcenter.`enum`.LogCategory
import com.flink.logcenter.entity.{CountLog, LogEntity}
import com.flink.logcenter.sink._
import com.flink.logcenter.task.time.CronTask
import com.flink.logcenter.util.KafkaUtil
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.{CollectionUtil, Collector}


object LogCenterTask {
  def main(args: Array[String]): Unit = {

//    CronTask.run

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000L)
    env.setParallelism(1)

    val properties = KafkaUtil.getProperties("behavior_log")

    val browseTag = new OutputTag[LogEntity]("BROWSE_SIDE_OUTPUT")
    val seeTag = new OutputTag[LogEntity]("SEE_SIDE_OUTPUT")
    val likeTag = new OutputTag[LogEntity]("LIKE_SIDE_OUTPUT")
    val shareTag = new OutputTag[LogEntity]("SHARE_SIDE_OUTPUT")
    val commentTag = new OutputTag[LogEntity]("COMMENT_SIDE_OUTPUT")
    val collectionTag = new OutputTag[LogEntity]("COLLECTION_SIDE_OUTPUT")
    val relayTag = new OutputTag[LogEntity]("RELAY_SIDE_OUTPUT")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("behavior_log", new SimpleStringSchema(), properties)).filter(_.nonEmpty)

    val sideStream = dataStream.flatMap((logString: String, out: Collector[LogEntity]) => {
      try {
        val jSONObject = JSONUtil.parseObj(logString)
        val contents = jSONObject.getJSONArray("contents")
        val behaviorType = jSONObject.getStr("behaviorType")
        val areaName = jSONObject.getStr("areaName")
        val areaCode = jSONObject.getStr("areaCode")
        val userId = jSONObject.getStr("userId")
        val timestamp = jSONObject.getLong("timestamp")

        if (!CollectionUtil.isNullOrEmpty(contents)) {
          contents.forEach(e => {
            val json = e.asInstanceOf[JSONObject]
            val logEntity = LogEntity(json.getStr("contentId"), json.getStr("createById"), behaviorType, areaName, areaCode, userId, 1L, timestamp)
            out.collect(logEntity)
          })
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace
        }
      }
    }).filter(data => StrUtil.isNotEmpty(data.contentId)).process((i: LogEntity, context: ProcessFunction[LogEntity, String]#Context, collector: Collector[String]) => {
      if (StrUtil.isNotEmpty(i.contentId)) {
        if (LogCategory.BROWSE.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(browseTag, i)
        }
        if (LogCategory.SEE.toString().equalsIgnoreCase(i.behaviorType)) {
          //          ContentHBaseUtil.saveContentUserLog(i.contentId, i.userId, 1)
          context.output(seeTag, i)
        }
        if (LogCategory.LIKE.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(likeTag, i)
        }
        if (LogCategory.SHARE.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(shareTag, i)
        }
        if (LogCategory.COMMENT.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(commentTag, i)
        }
        if (LogCategory.COLLECTION.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(collectionTag, i)
        }
        if (LogCategory.RELAY.toString().equalsIgnoreCase(i.behaviorType)) {
          context.output(relayTag, i)
        }
      }

    })


    val time = Time.hours(1L)
    val areaTime = Time.minutes(5L)


    val contentStream: DataStream[LogEntity] = sideStream.getSideOutput(browseTag)
    val seeStream: DataStream[LogEntity] = sideStream.getSideOutput(seeTag)
    val likeStream: DataStream[LogEntity] = sideStream.getSideOutput(likeTag)
    val shareStream: DataStream[LogEntity] = sideStream.getSideOutput(shareTag)
    val commentStream: DataStream[LogEntity] = sideStream.getSideOutput(commentTag)
    val collectionStream: DataStream[LogEntity] = sideStream.getSideOutput(collectionTag)
    val relayStream: DataStream[LogEntity] = sideStream.getSideOutput(relayTag)


    logSink(contentStream, time, areaTime, LogCategory.BROWSE.toString)
    logSink(seeStream, time, areaTime, LogCategory.SEE.toString)
    logSink(likeStream, time, areaTime, LogCategory.LIKE.toString)
    logSink(shareStream, time, areaTime, LogCategory.SHARE.toString)
    logSink(commentStream, time, areaTime, LogCategory.COMMENT.toString)
    logSink(collectionStream, time, areaTime, LogCategory.COLLECTION.toString)
    logSink(relayStream, time, areaTime, LogCategory.RELAY.toString)


    env.execute("内容日志实时统计任务")
  }


  def logSink(ds: DataStream[LogEntity], time: Time, areaTime: Time, logType: String): Unit = {

    val contentAreaTag = new OutputTag[LogEntity]("CONTENT_AREA_SIDE_OUTPUT")
    val fiveContentAreaTag = new OutputTag[LogEntity]("5_CONTENT_AREA_SIDE_OUTPUT")
    val contentCountTag = new OutputTag[LogEntity]("CONTENT_COUNT_SIDE_OUTPUT")
    val authorCountTag = new OutputTag[LogEntity]("AUTHOR_COUNT_SIDE_OUTPUT")
    val areaCountTag = new OutputTag[LogEntity]("AREA_COUNT_SIDE_OUTPUT")

    val stream = ds.process((i: LogEntity, context: ProcessFunction[LogEntity, String]#Context, collector: Collector[String]) => {
      context.output(contentAreaTag, i)
      context.output(contentCountTag, i)
      context.output(authorCountTag, i)
      context.output(areaCountTag, i)
      context.output(fiveContentAreaTag, i)
    })

    stream.getSideOutput(contentAreaTag).filter(data => StrUtil.isNotEmpty(data.contentId) && StrUtil.isNotBlank(data.areaCode))
      .keyBy(log => (log.contentId, log.areaCode))
      .window(TumblingProcessingTimeWindows.of(time)).sum("countNum")
      .addSink(new LogContentAreaMysqlSink(logType))

    stream.getSideOutput(fiveContentAreaTag).filter(data => StrUtil.isNotEmpty(data.contentId) && StrUtil.isNotBlank(data.areaCode))
      .keyBy(log => (log.contentId, log.areaCode))
      .window(TumblingProcessingTimeWindows.of(areaTime)).sum("countNum")
      .addSink(new LogFiveContentAreaMysqlSink(logType))

    stream.getSideOutput(contentCountTag).filter(data => StrUtil.isNotEmpty(data.contentId) && StrUtil.isNotEmpty(data.createById))
      .keyBy(_.contentId).window(TumblingProcessingTimeWindows.of(time))
      .aggregate(new TupleCountAgg, new TupleWindowCountResult)
      .addSink(new LogContentCountMySqlSink(logType))


    stream.getSideOutput(authorCountTag).filter(data => StrUtil.isNotEmpty(data.createById))
      .keyBy(_.createById).window(TumblingProcessingTimeWindows.of(time))
      .aggregate(new CountAgg, new WindowCountResult)
      .addSink(new LogAuthorCountMySqlSink(logType))


    stream.getSideOutput(areaCountTag).filter(data => StrUtil.isNotEmpty(data.areaCode))
      .keyBy(_.areaCode).window(TumblingProcessingTimeWindows.of(time))
      .sum("countNum")
      .addSink(new LogAreaCountMySqlSink(logType))
  }


  class CountAgg extends AggregateFunction[LogEntity, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: LogEntity, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowCountResult extends WindowFunction[Long, CountLog, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, aggregateResult: Iterable[Long], out: Collector[CountLog]): Unit = {
      val next = aggregateResult.iterator.next()
      out.collect(CountLog(next, key, "", window.getEnd, 0L))
    }
  }

  class TupleCountAgg extends AggregateFunction[LogEntity, (String, Long, Long), (String, Long, Long)] {
    override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

    override def add(value: LogEntity, accumulator: (String, Long, Long)): (String, Long, Long) = (value.createById, accumulator._2 + 1L, value.timestamp)

    override def getResult(accumulator: (String, Long, Long)): (String, Long, Long) = accumulator

    override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = (a._1, a._2 + b._2, a._3)
  }

  class TupleWindowCountResult extends WindowFunction[(String, Long, Long), CountLog, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, aggregateResult: Iterable[(String, Long, Long)], out: Collector[CountLog]): Unit = {
      val next = aggregateResult.iterator.next()
      out.collect(CountLog(next._2, key, next._1, window.getEnd, next._3))
    }
  }


}
