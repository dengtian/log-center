package com.flink.logcenter.task.time

import java.{lang, util}

import cn.hutool.core.collection.CollUtil
import cn.hutool.core.date.{DateField, DateUtil}
import cn.hutool.core.map.MapUtil
import cn.hutool.core.thread.ThreadUtil
import cn.hutool.core.util.StrUtil
import cn.hutool.cron.CronUtil
import com.flink.logcenter.entity.{ContentUserLog, DataLabels, Label}
import com.flink.logcenter.util._

import scala.collection.JavaConverters._
import scala.collection.mutable

object CronTask {

  def main(args: Array[String]): Unit = {
    //测试用
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0")
    dataLabel
  }

  def run(): Unit = {
    CronUtilProxy.schedule("0 */10 * * * *", () => {
      println("{} 协同过滤任务开始", DateUtil.now)

      val logs: util.List[ContentUserLog] = ContentHBaseUtil.getContentUserLog

      if (CollUtil.isNotEmpty(logs)) {
        val logMap: util.Map[String, util.Set[String]] = new util.HashMap[String, util.Set[String]]
        println("获取内容点击记录 {} 条", logs.size)
        logs.forEach((log: ContentUserLog) => {
          if (StrUtil.isNotEmpty(log.getContentId) && StrUtil.isNotEmpty(log.getUserId)) {
            var set: util.Set[String] = logMap.get(log.getContentId)
            if (set == null) set = new util.HashSet[String]
            set.add(log.getUserId)
            logMap.put(log.getContentId, set)
          }
        })
        logMap.forEach((k: String, v: util.Set[String]) => {

          var map = mutable.HashMap[String, Double]()

          logMap.forEach((ki: String, vi: util.Set[String]) => {
            if (k != ki) {
              val score = ItemCoeff.getItemCoeffScore(v.asScala, vi.asScala)
              map += (ki -> score)
            }
          })

          var reverse = map.toList.filter(_._2 > 0).sortBy(_._2).reverse
          if (reverse.size > 20) {
            reverse = reverse.take(20)
          }

          val hashMap = new util.HashMap[String, lang.Double]()
          reverse.toMap.foreach(e => {
            hashMap.put(e._1, e._2)
          })

          ContentHBaseUtil.saveItemCoeffRelate(k, hashMap)

        })


      }
      println("{} 协同过滤任务结束", DateUtil.now)
    })

    // 支持秒级别定时任务
    CronUtil.setMatchSecond(true)
    CronUtil.start()
  }

  def dataLabel(): Unit = {

    CronUtilProxy.schedule("0 */10 * * * *", () => {

      //获取近一段时间内有记录的用户id
      val logs = HBaseClient.getAllKeyByTime(HBaseConst.USER_LOG, DateUtil.offset(new java.util.Date, DateField.MINUTE, -10).getTime, DateUtil.current(false))

      if (CollUtil.isNotEmpty(logs)) {

        //获取所有data_labels中的数据
        val dataLabels = ContentHBaseUtil.getAllDataLabels


        val dlMap = new mutable.HashMap[String, DataLabels]()

        if (CollUtil.isNotEmpty(dataLabels)) {

          //按照type给data_labels数据分类
          val dataMap = mutable.HashMap.empty[String, mutable.Set[DataLabels]]
          dataLabels.forEach(e => {
            val option = dataMap.get(e.getType)

            if (option != None) {
              val set = option.get
              if (!set.isEmpty) {
                set += e
                dataMap.put(e.getType, set)
              }
            } else {
              dataMap.put(e.getType, mutable.Set[DataLabels](e))
            }


            val value = e.getDataId.replace(e.getType + "_", "")
            dlMap += (value -> e)
          })

          logs.forEach(log => {

            //更新用户标签

            if (CollUtil.isNotEmpty(log.getDataIds)) {
              val labels = new mutable.HashMap[String, (Int, String)]()
              log.getDataIds.forEach(dataId => {
                val option = dlMap.get(dataId)
                if (option != None) {
                  val typeStr = option.get.getType
                  val la = option.get.getLabels
                  if (CollUtil.isNotEmpty(la)) {
                    la.forEach(l => {
                      labels += (l.getLabel -> (l.getWeight, typeStr))
                    })
                  }
                }
              })
              if (labels.size > 0) {
                labels.foreach(e => {
                  try {
                    HBaseClient.increaseColumn(HBaseConst.USER_LABEL, log.getUserId, e._2._2, e._1, e._2._1)
                  } catch {
                    case ex: Exception =>
                      ex.printStackTrace
                  }
                })
              }
            }


            //获取用户标签
            val map = ContentHBaseUtil.getUserLabels(log.getUserId)
            if (MapUtil.isNotEmpty(map)) {
              map.forEach((k, v) => {
                ThreadUtil.execute(() => {
                  val option = dataMap.get(k)
                  if (option != None) {
                    val set = option.get
                    val ids = new mutable.ListBuffer[(String, Double)]
                    for (elem <- set) {

                      val labelSet = mutable.Set[Label]()
                      elem.getLabels.forEach(label => {
                        labelSet += label
                      })

                      val score = LabelCoeff.getScore(v.asScala, labelSet)
                      if (score > 0) {
                        ids += (elem.getDataId -> score)
                      }
                    }

                    if (ids.nonEmpty) {

                      ids.sortWith((a, b) => a._2 > b._2)


                      val idBuffer = new mutable.ListBuffer[String]

                      ids.take(100).map(e => idBuffer += e._1.replace(k + "_", ""))


                      val str = idBuffer.mkString(",")


                      HBaseClient.deleteData(HBaseConst.USER_RELATE, log.getUserId, "data", k)
                      HBaseClient.putData(HBaseConst.USER_RELATE, log.getUserId, "data", k, str)
                    }

                  }
                })
              })
            }
          })
        }
      }

    })

  }

}
