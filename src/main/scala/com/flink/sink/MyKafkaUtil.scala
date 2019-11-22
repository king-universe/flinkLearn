package com.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer011}

/**
  * @author 王犇
  * @date 2019/11/22 10:25
  * @version 1.0
  */
class MyKafkaUtil {

  val brokerList = "hadoop102:9092";

  def getProduct(topic: String): FlinkKafkaProducer011[String] = {
    new FlinkKafkaProducer011[String](brokerList, topic, new SimpleStringSchema())
  }
}
