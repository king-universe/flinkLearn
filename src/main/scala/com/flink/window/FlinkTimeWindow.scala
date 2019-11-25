package com.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * @author 王犇
  * @date 2019/11/25 15:17
  * @version 1.0
  */
object FlinkTimeWindow {

  def main(args: Array[String]): Unit = {
    val tools:ParameterTool=ParameterTool.fromArgs(args);
   val see:StreamExecutionEnvironment= StreamExecutionEnvironment.getExecutionEnvironment
    val host=tools.get("host")
    val port=tools.get("port").toInt
    val stream:DataStream[String]=see.socketTextStream(host,port)

    import org.apache.flink.api.scala._
    val keyedStream: KeyedStream[(String, Int), Tuple] =stream.map(str=>(str,1)).keyBy(0)
//    每10统计一次各个渠道的计数
    val  windowedGunStream:WindowedStream[(String,Int),Tuple,TimeWindow]=keyedStream.timeWindow(Time.seconds(10))

    windowedGunStream.sum(1).print("这是Time滚动窗口：")

//    每5秒统计一次最近10秒的各个渠道的计数
    val  windowedHuaStream:WindowedStream[(String,Int),Tuple,TimeWindow]= keyedStream.timeWindow(Time.seconds(10),Time.seconds(5))


    windowedHuaStream.sum(1).print("这是Time滑动窗口：")


    see.execute()
  }
}
