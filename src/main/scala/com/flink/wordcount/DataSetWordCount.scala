package com.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem

/**
  * @author 王犇
  * @date 2019/11/20 13:07
  * @version 1.0
  */
object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val tools: ParameterTool = ParameterTool.fromArgs(args)

    val input = tools.get("input")

    val output=tools.get("output")

    //构造执行环境
    val env:ExecutionEnvironment=ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val ds:DataSet[String]=env.readTextFile(input)
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    //经过groupby进行分组，sum进行聚合
    val aggDs: AggregateDataSet[(String, Int)] =ds.flatMap(_.split("\t")).map((_,1)).groupBy(0).sum(1);

    aggDs.writeAsText(output,FileSystem.WriteMode.OVERWRITE)

    env.execute()

  }

}
