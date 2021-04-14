package com.flink.logcenter.task

import java.util

import cn.hutool.core.collection.CollUtil
import cn.hutool.core.thread.ThreadUtil
import cn.hutool.core.util.StrUtil
import cn.hutool.json.{JSONObject, JSONUtil}
import com.flink.logcenter.entity.{Content, Label, Log}
import com.flink.logcenter.util.{ContentHBaseUtil, HBaseClient, HBaseConst, KafkaUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ContentPublishTask {
  def main(args: Array[String]): Unit = {
    //测试用
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000L)
    env.setParallelism(1)

    val properties = KafkaUtil.getProperties("content_publish")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("content_publish", new SimpleStringSchema(), properties)).filter(_.nonEmpty)

    dataStream.flatMap((log: String, out: Collector[Content]) => {
      try {
        val jSONObject = JSONUtil.parseObj(log)
        val labels = jSONObject.getJSONArray("labels")
        val timestamp = jSONObject.getStr("timestamp")
        val fileType = jSONObject.getStr("fileType")
        val authorId = jSONObject.getStr("authorId")
        val authorArea = jSONObject.getStr("authorArea")
        val contentId = jSONObject.getStr("contentId")

        val content = new Content

        content.setContentId(contentId)
        content.setAuthorArea(authorArea)
        content.setAuthorId(authorId)
        content.setFileType(fileType)
        content.setTimestamp(timestamp)

        if (labels != null) {

          val list = new util.ArrayList[Label]()

          labels.forEach(e => {
            val label = e.asInstanceOf[JSONObject]
            val labelName = label.getStr("label")
            val weight = label.getInt("weight")
            list.add(new Label(labelName, weight))
          })

          content.setLabels(list)
          out.collect(content)
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace
        }
      }

    }).filter(content => StrUtil.isNotEmpty(content.getContentId)).map(content => {


      ThreadUtil.execute(() => {

        try {
          HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId, "attr", "contentId", content.getContentId)
          HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId, "attr", "timestamp", content.getTimestamp)
          HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId, "attr", "fileType", content.getFileType)
          HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId, "author", "authorId", content.getAuthorId)
          HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId, "author", "authorArea", content.getAuthorArea)
        } catch {
          case ex: Exception => {
            ex.printStackTrace
          }
        }
      })

      ThreadUtil.execute(() => {
        if (CollUtil.isNotEmpty(content.getLabels)) {
          content.getLabels.forEach(e => {
            if (StrUtil.isNotEmpty(e.getLabel)) {
              HBaseClient.increaseColumn(HBaseConst.CONTENT_TABLE, content.getContentId, "label", e.getLabel, e.getWeight)
            }
          })
//          ContentHBaseUtil.calculateCoeffAndSave(content, 10, 30)
        }
      })

    })

    env.execute("内容发布任务")
  }

}
