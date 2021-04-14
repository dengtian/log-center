package com.flink.logcenter.entity

case class LogEntity(contentId: String, createById: String, behaviorType: String, areaName: String, areaCode: String, userId: String, countNum: Long, timestamp: Long)
