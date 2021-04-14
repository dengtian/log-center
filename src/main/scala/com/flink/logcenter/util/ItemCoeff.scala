package com.flink.logcenter.util

import scala.collection.mutable

object ItemCoeff {

  def getItemCoeffScore(set1: mutable.Set[String], set2: mutable.Set[String]): Double = {
    if (set1.nonEmpty && set2.nonEmpty) {
      val total = math.sqrt(set1.size * set2.size)
      val set = set1.&(set2)
      return set.size / total
    }
    0f
  }

}
