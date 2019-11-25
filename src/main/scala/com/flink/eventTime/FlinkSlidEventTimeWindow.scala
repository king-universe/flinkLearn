package com.flink.eventTime

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

/**
  * @author 王犇
  * @date 2019/11/25 17:24
  * @version 1.0
  */
object FlinkSlidEventTimeWindow {
  def main(args: Array[String]): Unit = {
    val tools: ParameterTool = ParameterTool.fromArgs(args);
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val host = tools.get("host")
    val port = tools.get("port").toInt
    val stream: DataStream[String] = see.socketTextStream(host, port)

    import org.apache.flink.api.scala._

    val textWithTsDstream: DataStream[(String, Long, Int)] = stream.map(
      str => {
        val array: Array[String] = str.split(",")
        (array(0), array(1).toLong, 1)
      }
    )

    val textWithEventTimeDstream :DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(1)) {
      override def extractTimestamp(element: (String, Long, Int)): Long = {
        element._2
      }
    })

    val textKeyedStream:KeyedStream[(String,Long,Int),Tuple]=textWithEventTimeDstream.keyBy(0)

    textKeyedStream.print("textKeys:")


    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyedStream.window(SlidingEventTimeWindows.of(Time.seconds(2),Time.milliseconds(500)))

    windowStream.sum(1).print("sum:")

    val groupDstream: DataStream[mutable.HashSet[Long]] = windowStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count)) =>
      set += ts
    }

    groupDstream.print("window::::").setParallelism(1)

    see.execute()
  }
}
