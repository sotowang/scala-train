//package com.soto.scala
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * SQLContext的使用
//  * 注意：IDEA是在本地，而测试数据是在服务器上，能不能在本地开发测试
//  */
//object SQLContextApp {
//  def main(args: Array[String]): Unit = {
//
//    val path =args(0)
//
//    //1.创建相应的Context
//    val sparkConf = new SparkConf()
//
//    sparkConf.setAppName("SQLContextApp")
//
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new SQLContext(sc)
//
//
//    //2.相关处理:json
//    val people = sqlContext.read.format("json").load("/home/sotowang/user/aur/spark/spark-1.6.0-cdh5.7.0/examples/src/main/resources/people.json")
//    people.printSchema()
//    people.show()
//
//    //3.关闭资源
//    sc.stop()
//  }
//}
