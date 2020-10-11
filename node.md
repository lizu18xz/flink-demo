

### 计数器
```text
 getRuntimeContext.addAccumulator("ele-counts-scala", counter)
 
 val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")


 缓存:
 // step1: 注册一个本地/HDFS文件
    env.registerCachedFile(filePath, "pk-scala-dc")

 override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("pk-scala-dc")

        val lines = FileUtils.readLines(dcFile)  // java


        /**
          * 此时会出现一个异常：java集合和scala集合不兼容的问题
          */
        import scala.collection.JavaConverters._
        for(ele <- lines.asScala){ // scala
          println(ele)
        }
      }
```

### 自定义数据源
```text
底层肯定会进行env.addSource()

SourceFunction:
不能并行处理

ParallelSourceFunction:
并行的方式执行


RichParallelSourceFunction

```


### window
```text
window
带key

windowAll
不带key,并行度为1


窗口分配器:
Assigners
一个元素有可能被分配到一到多个窗口,flink提供的窗口如下:
tumbling windows 滚动窗口  TumblingProcessingTimeWindows
sliding windows  滑动窗口  SlidingProcessingTimeWindows

也可以自己实现
/**
   * Windows this [[KeyedStream]] into tumbling time windows.
   *
   * This is a shortcut for either `.window(TumblingEventTimeWindows.of(size))` or
   * `.window(TumblingProcessingTimeWindows.of(size))` depending on the time characteristic
   * set using
   * [[StreamExecutionEnvironment.setStreamTimeCharacteristic()]]
   *
   * @param size The size of the window.
   */ 
 简写如下:
timeWindow(size: Time)


/**
   * Windows this [[KeyedStream]] into sliding time windows.
   *
   * This is a shortcut for either `.window(SlidingEventTimeWindows.of(size))` or
   * `.window(SlidingProcessingTimeWindows.of(size))` depending on the time characteristic
   * set using
   * [[StreamExecutionEnvironment.setStreamTimeCharacteristic()]]
   *
   * @param size The size of the window.
   */
 简写如下:
  def timeWindow(size: Time, slide: Time): WindowedStream[T, K, TimeWindow] = {
    new WindowedStream(javaStream.timeWindow(size, slide))
  }
比如:每隔5分钟产生一个窗口，大小为10分钟
```


### window function
````text
指定窗口的计算
function
比如:AggregateFunction聚合function
增量执行的。

ProcessWindowFunction:
可以拿到窗口中所有的迭代器
但是性能资源开销比较多,不是增量的
public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
}
可以进行数据的排序


````












