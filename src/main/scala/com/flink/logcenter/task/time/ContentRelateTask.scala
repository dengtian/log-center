package com.flink.logcenter.task.time

import java.util.concurrent.LinkedBlockingQueue

import cn.hutool.core.collection.CollUtil
import cn.hutool.core.thread.ExecutorBuilder
import com.flink.logcenter.util.ContentHBaseUtil

object ContentRelateTask {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0")

    executeRelate
  }

  def executeRelate(): Unit = {
    val contents = ContentHBaseUtil.getContents(30)
    if (CollUtil.isNotEmpty(contents)) {

      val executor = ExecutorBuilder.create.setCorePoolSize(10).setMaxPoolSize(50).setWorkQueue(new LinkedBlockingQueue[Runnable](2000)).build

      contents.forEach(content => {
        executor.execute(() => ContentHBaseUtil.calculateRelate(content, contents))
      })
    }
  }

}
