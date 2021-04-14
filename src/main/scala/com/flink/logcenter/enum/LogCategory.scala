package com.flink.logcenter.enum

object LogCategory extends Enumeration {
  type category = Value
  val BROWSE = Value("BROWSE")
  val SEE = Value("SEE")
  val LIKE = Value("LIKE")
  val SHARE = Value("SHARE")
  val COMMENT = Value("COMMENT")
  val COLLECTION = Value("COLLECTION")
  val RELAY = Value("RELAY")
}
