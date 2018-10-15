package com.soto.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("saprk.ui.showConsoleProgress","false")
    println("开始运行wordcount")
    val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[4]"))

    println("开始读取文本文件")

    val textFile = sc.textFile("file:///home/sotowang/Desktop/test.txt")

    println("开始创建RDD")

    val countsRDD = textFile.flatMap(line=>line.split(" "))
      .map(word => (word,1))
      .reduceByKey(_+_)

    println("开始保存到文本文件")
    try{
      countsRDD.saveAsTextFile("file:///home/sotowang/Desktop/output")
      println("存盘成功")
    }
    catch{
      case e:Exception => println("目录已存在,请先删除原有目录")
    }
  }

}
