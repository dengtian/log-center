package com.flink.logcenter.util

import cn.hutool.core.collection.CollUtil
import com.flink.logcenter.entity.Content

import scala.collection.mutable

object ContentCoeff {

  def getLabelScore(c1: Content, c2: Content): Double = {
    val labels1 = c1.getLabels
    val labels2 = c2.getLabels
    if (CollUtil.isEmpty(labels1) || CollUtil.isEmpty(labels2)) {
      0f
    } else {
      val sqrt = Math.sqrt(c1.getTotal + c2.getTotal)
      if (sqrt == 0) return 0.0
      val map = mutable.HashMap.empty[String, Int]

      labels2.forEach(e => {
        map.put(e.getLabel, e.getWeight)
      })

      var total: Double = 0f

      labels1.forEach(e => {
        val weight = map.get(e.getLabel)
        if (weight != None) {
          total += weight.get * e.getWeight
        }
      })

      Math.sqrt(total) / sqrt
    }
  }

  def getSimilarity(c1: Content, c2: Content):Double={
    val score = getLabelScore(c1, c2)
    val similarity = TextUtil.getSimilarity(c1.getTitle, c2.getTitle)
    println(score)
    println(similarity)
    score
  }



}
