package com.flink.sink

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig, FlinkJedisSentinelConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.JavaConverters._

/**
  * @author 王犇
  * @date 2019/11/22 14:28
  * @version 1.0
  */
class MyRedisUtil extends  Serializable {
  //  val conf=new FlinkJedisPoolConfig.Builder().setHost("192.168.88.47").setHost("6379").build()


  val sentinel: Set[String] = Set("192.168.88.47:26379", "192.168.8.180:26379")

  val conf = new FlinkJedisSentinelConfig.Builder().setMasterName("master").setSentinels(sentinel.asJava).setPassword("123456").setSoTimeout(50000).build()

  def getRedisSink: RedisSink[(String, String)] = {
    new RedisSink[(String, String)](conf, new MyRedisMapper)
  }

  class MyRedisMapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"channel_count")
    }

    override def getKeyFromData(data: (String, String)): String = {data._1}

    override def getValueFromData(data: (String, String)): String = {data._2}
  }

}
