package com.flink.logcenter

import java.util.Properties

import cn.hutool.core.util.RandomUtil
import cn.hutool.json.{JSONArray, JSONObject, JSONUtil}
import com.flink.logcenter.`enum`.LogCategory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "172.16.14.132:9092") // 指定响应方式
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

    for (i <- 1 to 10) {
      val content: JSONObject = new JSONObject
      val contents: JSONArray = new JSONArray
      content.set("contentId", "contentId00" + i)
      content.set("createById", "creater" + i)

      contents.add(content)

      val log: JSONObject = new JSONObject

      log.set("contents", contents)
      log.set("areaCode", "120104")
      log.set("timestamp", System.currentTimeMillis)
      log.set("behaviorType", LogCategory.SEE.toString)
      val str = "userId" + (i + RandomUtil.randomInt(10, 20))
      println(str)
      log.set("userId", str)


      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String]("behavior_log", JSONUtil.toJsonStr(log))).get()

      println(rmd.toString)
    }

    producer.close()
  }

}
