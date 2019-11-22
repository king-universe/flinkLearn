package com.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author 王犇
  * @date 2019/11/20 13:16
  * @version 1.0
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val tools: ParameterTool = ParameterTool.fromArgs(args)
    val host = tools.get("host")
    val port = tools.get("port").toInt

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream(host, port)


    // flatMap和Map需要引用的隐式转换
    import org.apache.flink.api.scala._

    //处理 分组并且sum聚合
    val dStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\t")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)
    dStream.print()

    env.execute()
  }
}
