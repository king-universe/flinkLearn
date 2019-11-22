package com.flink.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
  * @author 王犇
  * @date 2019/11/21 14:26
  * @version 1.0
  */
object KeyByAndReduce {
  def main(args: Array[String]): Unit = {
    val tools = ParameterTool.fromArgs(args);
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream:DataStream[String]=see.socketTextStream(tools.get("host"),tools.get("port").toInt)

    import org.apache.flink.api.scala._


    val keyedStream: KeyedStream[(String, Int), Tuple]=stream.flatMap(_.split(",")).filter(_.length>3).map(Tuple2(_,1)).keyBy(0)

    keyedStream.print()


    val startUplogDstream=keyedStream.reduce((ch1,ch2)=>(ch1._1,ch2._2+ch1._2)).print().setParallelism(3)


    see.execute()

  }
}
