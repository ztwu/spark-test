package com.iflytek.edcc.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by admin on 2017/12/1.
  */

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/12/1
  * Time: 10:37
  * Description
  */

object ScalaTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val data1 = sc.makeRDD(1 until 100, 1)
    data1.foreach(println)

    //Map 有两种类型，可变与不可变，区别在于可变对象可以修改它，而不可变对象不可以。
    //默认情况下 Scala 使用不可变 Map。如果你需要使用可变集合，你需要显式的引入 import scala.collection.mutable.Map 类

    //不可变map
    var map : Map[String,String] = Map()
    map += ("name"->"ztwu2")
    map += ("age"->"24")
    map.foreach(println)
    for (elem <- map) {
      println(elem._1 + "-" + elem._2)
    }

    //可变map,可变对象可以修改它
    var map2 : mutable.Map[String, String] = mutable.Map()
    map2 += ("name"->"ztwu2")
    map2 += ("age"->"24")
    map2("name") = "ztwu1"
    map2("age") = "21"
    for((key,value) <- map2) {
      println(key + "-" + value)
    }

    //可变list
    var listbuffer = ListBuffer[String]()
    listbuffer += "7"
    listbuffer += "8"
    listbuffer += "9"
//    for (i <- listbuffer){
//      println(i)
//    }
//    listbuffer.foreach(println)
    for (i <- 0 until listbuffer.length-1) {
      println(listbuffer(i))
    }

    //不可变list
    //列表是不可变的
    val list = List[String]("10","11","12","12")
    val listnew = list :+ "13"
    listnew.foreach(println)

    //Scala 集合分为可变的和不可变的集合。
    //默认情况下，Scala 使用的是不可变集合，如果你想使用可变集合，需要引用 scala.collection.mutable.Set 包
    var setbuffer = mutable.Set[String]()
    setbuffer += "14"
    setbuffer += "15"
    setbuffer += "15"
    for (elem <- setbuffer) {
      println(elem)
    }

    val set = Set[String]("16","17","18")
    set.foreach(println)

    //元组是不可变的
    val tuple = ("19","20")

    //可变数组
    var arraybuffer : ArrayBuffer[String] = ArrayBuffer[String]()
    arraybuffer += "1"
    arraybuffer += "2"
    arraybuffer += "3"
    arraybuffer.foreach(println)

    //不可变数组
    val array = Array[String]("4","5","6")
    array.foreach(println)

    //map元素操作
    val map11 = Map[String,String]("name1"->"ztwu1")
    val map21 = Map[String,String]("name2"->"ztwu3")
    val map31 = Map[String,String]("name2"->"ztwu2")
    val map111 = map11.+("name3"->"ztwu4")
    map111.++(map21).++(map31).foreach(println)

    val array11 = Array(1,1,1)
    val array21 = Array(2,2,2)
    val array31 = Array(3,3,3)
    array11.++(array21).++(array31).foreach(println)
    array11.foreach(println)

    //flatten方法不仅限于列表，还可以用于其他序列 (Array、 ArrayBuffer、 Vector等
    //flatten方法在至少其他两种场景很有用。第一，因为字符串是字符的序列，可以把字符串的列表转变为一个字符的列表（字符串的列表--> 字符的列表）：
    //第二，因为Option可以被看作是包含零个或者一个元素的容器，flatten对于Some和None元素的序列很有用。它会将Some中的值取出来新建一个列表，然后丢掉None元素（flatten--> kill None）：
    println(Array(Map(1->2),Map(3->4),Map(5->6)).flatten.toBuffer)
    println(Array(Map(1->2),Map(3->4),Map(5->6)).toBuffer)

    println(Array(List(1,2),List(3,4),List(5,6)).flatten.toBuffer)
    println(Array(List(1,2),List(3,4),List(5,6)).toBuffer)

    println(Array(Array(1,2),Array(3,4),Array(5,6)).flatten.toBuffer)
    println(Array(Array(1,2),Array(3,4),Array(5,6)).toBuffer)

    println(Array(Set(1,2),Set(3,4),Set(5,6)).flatten.toBuffer)
    println(Array(Set(1,2),Set(3,4),Set(5,6)).toBuffer)

    val x = List(Some(1,2), None, Some(3), None)
    x.foreach(x=>println(x.getOrElse(0)))

    val some = Some(1,2)
    println(some.get)

  }
}
