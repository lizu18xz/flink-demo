package com.imooc.flink.course05

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

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  //设置了并行度后 setParallelism(3)  run方法会同时执行3次
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }
}
