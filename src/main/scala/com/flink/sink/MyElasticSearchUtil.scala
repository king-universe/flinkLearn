package com.flink.sink

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import scala.collection.JavaConverters._

/**
  * @author 王犇
  * @date 2019/11/22 15:01
  * @version 1.0
  */
class MyElasticSearchUtil {

  def getElasticSearchSink(indexName: String) = {
    val httpHosts: List[HttpHost] = List(new HttpHost("hadoop102", 9200, "http"),
      new HttpHost("hadoop103", 9200, "http"),
      new HttpHost("hadoop104", 9200, "http"))


    val esFunc = new ElasticsearchSinkFunction[String] {
      override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("试图保存：" + element)
        val jsonObj: JSONObject = JSON.parseObject(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonObj)
        requestIndexer.add(indexRequest)
        println("保存1条")
      }
    }
    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts.asJava, esFunc)

    //刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)


    sinkBuilder.build()
  }

}
