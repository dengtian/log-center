package com.flink.logcenter.sink

import cn.hutool.core.date.DateUtil
import cn.hutool.db.{Db, Entity}
import cn.hutool.json.JSONUtil
import com.flink.logcenter.entity.LogEntity
import com.flink.logcenter.util.AreaUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class LogContentAreaMysqlSink extends RichSinkFunction[LogEntity] {
  var logType: String = ""

  def this(logType: String) {
    this()
    this.logType = logType
  }

  override def invoke(value: LogEntity, context: SinkFunction.Context[_]): Unit = {
    var hour = DateUtil.thisHour(true) - 1


    println("往mysql记录数据:" + DateUtil.now() + " hour :" + hour)

    if (hour >= 0 && this.logType.nonEmpty) {
      val area = AreaUtil.getByCode(value.areaCode)
      var province: String = null
      var district: String = null
      var county: String = null
      if (area != null) {
        province = area.getProvince
        district = area.getDistrict
        county = area.getCounty
      }


      println(JSONUtil.toJsonStr(area))

      val i = Db.use().count(Entity.create("content_area_hour")
        .set("content_id", value.contentId)
        .set("behavior_type", logType.toLowerCase())
        .set("area_code", value.areaCode)
        .set("hour", hour)
        .set("date", DateUtil.today)
      )

      if (i < 1) {
        Db.use().insert(Entity.create("content_area_hour")
          .set("area_code", value.areaCode)
          .set("province", province)
          .set("district", district)
          .set("county", county)
          .set("content_id", value.contentId)
          .set("behavior_type", logType.toLowerCase())
          .set("count_num", value.countNum)
          .set("hour", hour)
          .set("date", DateUtil.today())
        )
      }

    }
  }
}
