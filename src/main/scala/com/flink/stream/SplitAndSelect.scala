package com.flink.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
  * @author 王犇
  * @date 2019/11/21 16:40
  * @version 1.0
  */
object SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val tools = ParameterTool.fromArgs(args);
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = see.socketTextStream(tools.get("host"), tools.get("port").toInt)
    stream.print()

    val splitStream: SplitStream[String] = stream.split { startUplog =>
      var flags: List[String] = null
      if ("appstore" == startUplog.toString) {
        flags = List(startUplog.toString)
      } else {
        flags = List("other")
      }
      flags
    }

    val appStoreStream: DataStream[String] = splitStream.select("appstore")
    appStoreStream.print("apple:").setParallelism(1)
    val otherStream: DataStream[String] = splitStream.select("other")
    otherStream.print("other:").setParallelism(1)

    see.execute()
  }
}
