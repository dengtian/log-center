package com.flink.logcenter.sink

import java.util.Date

import cn.hutool.core.date.DateUtil
import cn.hutool.core.util.StrUtil
import com.flink.logcenter.entity.LogEntity
import com.flink.logcenter.util.{AreaUtil, RedisUtil, TimeUtil}

object RedisSink {

  def invoke(value: LogEntity, logType: String): Unit = {
    var hour = 0l
    if (value.timestamp > 0) {
      hour = DateUtil.hour(new Date(value.timestamp), true)
    } else {
      hour = DateUtil.thisHour(true)
    }


    println("往redis记录数据:" + DateUtil.now() + " hour :" + hour)


    if (hour >= 0 && StrUtil.isNotEmpty(logType)) {
      val area = AreaUtil.getByCode(value.areaCode)
      var province: String = null
      var district: String = null
      var county: String = null
      if (area != null) {
        province = area.getProvince
        district = area.getDistrict
        county = area.getCounty
      }

      val endThisHour: Long = TimeUtil.getEndThisHour

      try {
        if (StrUtil.isNotEmpty(county)) {
          RedisUtil.hSetAndExpire("area_count_" + logType.toLowerCase + "_" + county, value.contentId, value.countNum, 300)
          RedisUtil.hIncreaseExpireAt("area_count_" + logType.toLowerCase + "_total_" + county, value.contentId, value.countNum, endThisHour)
        }

        if (StrUtil.isNotEmpty(district)) {
          RedisUtil.hSetAndExpire("area_count_" + logType.toLowerCase + "_" + district, value.contentId, value.countNum, 300)
          RedisUtil.hIncreaseExpireAt("area_count_" + logType.toLowerCase + "_total_" + district, value.contentId, value.countNum, endThisHour)
        }

        if (StrUtil.isNotEmpty(province)) {
          RedisUtil.hSetAndExpire("area_count_" + logType.toLowerCase + "_" + province, value.contentId, value.countNum, 300)
          RedisUtil.hIncreaseExpireAt("area_count_" + logType.toLowerCase + "_total_" + province, value.contentId, value.countNum, endThisHour)
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace
        }
      }

    }
  }
}
