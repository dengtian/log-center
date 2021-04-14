package com.flink.logcenter


import com.flink.logcenter.entity.{Content, Label}
import com.flink.logcenter.util.{ContentCoeff, NlpUtil, TextUtil}

object ContentCoeffTest {
  def main(args: Array[String]): Unit = {



    val value = "宋朝华：全力以赴夺取一季度“开门红”"
    val value2 = "宋朝华:咬定目标苦干实干 坚决打赢脱贫攻坚战"


    val keys = NlpUtil.getKeysByPredictOnlyWord(value, 5)
    val keys2 = NlpUtil.getKeysByPredictOnlyWord(value2, 5)

    println(keys)
    println(keys2)


    val content1: Content = new Content
    content1.setTitle(value)
    val labels: java.util.List[Label] = new java.util.ArrayList[Label]
    keys.forEach(e => {
      labels.add(new Label(e, 10))
    })

    content1.setLabels(labels)

    val content2: Content = new Content

    content2.setTitle(value2)

    val labels2: java.util.List[Label] = new java.util.ArrayList[Label]
    keys2.forEach(e => {
      labels2.add(new Label(e, 10))
    })

    content2.setLabels(labels2)



    println(ContentCoeff.getLabelScore(content1, content2))
    println(TextUtil.getSimilarity(content1.getTitle, content2.getTitle))

  }

}
