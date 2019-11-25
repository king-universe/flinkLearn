package com.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  * @author 王犇
  * @date 2019/11/25 15:50
  * @version 1.0
  */
object FlinkCountWindow {
  def main(args: Array[String]): Unit = {
    val tools:ParameterTool=ParameterTool.fromArgs(args);
    val see:StreamExecutionEnvironment= StreamExecutionEnvironment.getExecutionEnvironment
    val host=tools.get("host")
    val port=tools.get("port").toInt
    val stream:DataStream[String]=see.socketTextStream(host,port)

    import org.apache.flink.api.scala._
    val keyedStream: KeyedStream[(String, Int), Tuple] =stream.map(str=>(str,1)).keyBy(0)

    //每当某一个key的个数达到10的时候，显示出来
    val windowedGunStream: WindowedStream[(String, Int), Tuple, GlobalWindow]= keyedStream.countWindow(10)

    windowedGunStream.sum(1).print("这是count滚动窗口:")

    //每当某一个key的个数达到2的时候,触发计算，计算最近该key最近10个元素的内容
    val windowedHuaStream: WindowedStream[(String, Int), Tuple, GlobalWindow]= keyedStream.countWindow(10,2)

    windowedHuaStream.sum(1).print("这是count滑动窗口:")

    see.execute()
  }
}
