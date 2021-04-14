package com.flink.logcenter.sink

import cn.hutool.core.date.DateUtil
import cn.hutool.db.{Db, Entity}
import com.flink.logcenter.entity.CountLog
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class LogAuthorCountMySqlSink extends RichSinkFunction[CountLog] {

  var logType: String = ""

  def this(logType: String) {
    this()
    this.logType = logType
  }

  override def invoke(value: CountLog, context: SinkFunction.Context[_]): Unit = {
    var hour = DateUtil.thisHour(true) - 1

    if (hour >= 0 && this.logType.nonEmpty) {
      val i = Db.use().count(Entity.create("author_hour")
        .set("author_id", value.contentId)
        .set("behavior_type", logType.toLowerCase())
        .set("hour", hour)
        .set("date", DateUtil.today)
      )

      if (i < 1) {
        Db.use().insert(Entity.create("author_hour")
          .set("author_id", value.contentId)
          .set("behavior_type", logType.toLowerCase())
          .set("count_num", value.countNum)
          .set("hour", hour)
          .set("date", DateUtil.today())
        )
      }
    }
  }

}
