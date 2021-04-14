package com.flink.logcenter.task

import cn.hutool.core.thread.ThreadUtil
import cn.hutool.core.util.StrUtil
import cn.hutool.json.JSONUtil
import com.flink.logcenter.entity.Log
import com.flink.logcenter.util.{KafkaUtil, LogUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

class LogTask {
  def main(args: Array[String]): Unit = {
    //测试用
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000L)
    env.setParallelism(1)

    val properties = KafkaUtil.getProperties("label_log")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("label_log", new SimpleStringSchema(), properties)).filter(_.nonEmpty)

    dataStream.flatMap((logStr: String, out: Collector[Log]) => {
      try {
        val jSONObject = JSONUtil.parseObj(logStr)
        val logType = jSONObject.getStr("logType")
        val dataId = jSONObject.getStr("dataId")
        val userId = jSONObject.getStr("userId")

        val log = new Log

        log.setUserId(userId)
        log.setDataId(dataId)
        log.setLogType(logType)


        out.collect(log)

      } catch {
        case ex: Exception => {
          ex.printStackTrace
        }
      }

    }).filter(log => StrUtil.isNotEmpty(log.getUserId)).map(log => {

      ThreadUtil.execute(() => {
        LogUtil.saveLog(log)
      })


    })

    env.execute("内容发布任务")
  }
}
