package com.flink.logcenter

import java.util.Properties

import cn.hutool.db.{Db, Entity, Page}
import cn.hutool.json.{JSONObject, JSONUtil}
import com.flink.logcenter.entity.Label
import com.flink.logcenter.util.NlpUtil
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object ContentTest {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "localhost:9092") // 指定响应方式
    //prop.put("acks", "0")
    prop.put("acks", "all")
    // 请求失败重试次数
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "10000")

    // 得到生产者的实例
    val producer = new KafkaProducer[String, String](prop)


    val result = Db.use.page(Entity.create("core_content"), new Page(1, 2000))
    if (result.size > 0) result.forEach((e: Entity) => {
      val id = e.getStr("id")
      val title = e.getStr("title")
      val content = e.getStr("content")
      val fileType = e.getStr("fileType")
      val createById = e.getStr("createById")
      var list = new java.util.ArrayList[Label]()

      if (!StringUtils.isEmpty(title)) {
        val keys = NlpUtil.shortSegment(title)
        if (CollectionUtils.isNotEmpty(keys)) {
          keys.forEach(key => {
            list.add(new Label(key, 10))
          })
        }
      }
      if (!StringUtils.isEmpty(content)) {
        val keys = NlpUtil.shortSegment(content)
        if (CollectionUtils.isNotEmpty(keys)) {
          keys.forEach(key => {
            list.add(new Label(key, 1))
          })
        }
      }
      val json: JSONObject = new JSONObject
      json.set("timestamp", System.currentTimeMillis)
      json.set("fileType", fileType)
      json.set("authorId", createById)
      json.set("authorArea", "成都")
      json.set("contentId", id)

      json.set("labels", list)
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String]("content_publish", JSONUtil.toJsonStr(json))).get()
      println(JSONUtil.toJsonStr(rmd))
    })


    producer.close()
  }


}
