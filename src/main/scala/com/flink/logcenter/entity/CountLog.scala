package com.flink.logcenter.entity

case class CountLog(countNum: Long, contentId: String, createById: String, end: Long, eventTime: Long)
