package com.flink.sink

import java.sql
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * @author 王犇
  * @date 2019/11/22 15:46
  * @version 1.0
  */
class MyJdbcSinkUtil(sql:String) extends  RichSinkFunction[Array[Any]]{
  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://hadoop102:3306/test?useSSL=false"

  val username="root"

  val password="admin"

  val maxActive="20"

  var connection:Connection=null;
  //创建连接
  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)


    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()
  }


  //反复调用
  override def invoke(values: Array[Any]): Unit = {
    val ps:PreparedStatement = connection.prepareStatement(sql )
    println(values.mkString(","))
    for (i <- 0 until values.length) {
      ps.setObject(i + 1, values(i))
    }
    ps.executeUpdate()


  }

  override def close(): Unit = {

    if(connection!=null){
      connection.close()
    }

  }

}
