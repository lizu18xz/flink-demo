package com.imooc.flink.lz

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * Author: Michael PK
  */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{
  var isRunning = true
  var count = 1L
  override def cancel(): Unit = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = {
    System.out.println("open------")
    val total_task = getRuntimeContext.getNumberOfParallelSubtasks
    //根据当前的任务id号，决定当前任务应当读取mysql哪些数据行
    val subtask_index = getRuntimeContext.getIndexOfThisSubtask
    println(s"subtask_index = ${subtask_index}  total_task=${total_task}")

  }

  override def close(): Unit = super.close()


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning) {
      ctx.collect(count)
      count += 1
      System.out.println("ddddd")
      Thread.sleep(1000)
    }
  }
}
