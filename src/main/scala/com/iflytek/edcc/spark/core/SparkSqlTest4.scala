package com.iflytek.edcc.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/12/14
  * Time: 18:59
  * Description
  */

object SparkSqlTest4 {
  def main(args: Array[String]): Unit = {
    val data1 = Array(1,2,3,4,5)
    val data2 = Array("A","B","C","D","F")
    val data3 = Array("a","b","d","e","f")

    //for 循环 yield 会构造并返回与给定集合相同类型的集合
    val result = for{
        e1<-data1
        e2<-data2
        e3<-data3
       if(e1 != 1)
    } yield {
      (e1,e2, e3)
    }
    result.foreach(println)

    println("for循环scala测试1：")
    for {
      i <- 1 to 9
    }yield {
      for{
        j <- 1 to 9
      }{
        print(i+"*"+j+"="+i*j)
        print("\t")
      }
      println()
    }

    println("for循环scala测试2：")
    for {
      i <- 1 to 9
      j <- 1 to 9
    }yield {
      print(i+"*"+j+"="+i*j)
      print("\t")
      if(j == 9){
        println()
      }
    }

    val array1 = Array(0)
    val array2 = Array(1)
    println(data1(0).hashCode())
    println(data1(1).hashCode())
    println(array1.hashCode())
    println(array2.hashCode())

    val a= 1111111111
    println(a.hashCode())

    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List((1,"A"),(2,"B"),(3,"C")),2)
    val rdd2 = sc.parallelize(List((1,"a"),(2,"b"),(4,"c")),2)
    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)

    val rs1 = rdd1.reduceByKey((x,y)=>{x+y})
    println(rs1.getNumPartitions)

    val rs2 = rdd2.join(rdd1)
    println(rs2.getNumPartitions)

  }
}
