package com.flink.logcenter.util

import com.flink.logcenter.entity.Label

import scala.collection.mutable

object LabelCoeff {

  def getScore(set1: mutable.Set[Label], set2: mutable.Set[Label]): Double = {
    if (set1.nonEmpty && set2.nonEmpty) {
      var total1: Double = 0f
      var total2: Double = 0f

      set1.foreach(l => {
        total1 += math.pow(l.getWeight, 2)
      })

      set2.foreach(l => {
        total2 += math.pow(l.getWeight, 2)
      })

      if (total1 == 0 || total2 == 0) {
        0f
      } else {
        val map = mutable.HashMap.empty[String, Int]
        set2.foreach(l => {
          map.put(l.getLabel, l.getWeight)
        })

        var total: Double = 0f

        set1.foreach(l => {
          val option = map.get(l.getLabel)
          if (option != None) {
            total += option.get * l.getWeight
          }
        })

        math.sqrt(total) / math.sqrt(total1 + total2)
      }

    } else {
      0f
    }
  }

}
