package com.iflytek.edcc.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
//使用表达式，如开窗函数
//引用函数

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/4/13
  * Time: 11:47
  * Description
  */

object sparkRddTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("rdd1")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    //从内存中创建rdd，既对数据并行化
    val rdd1 = sc.parallelize(List(1, 2, 3, 4), 2)
    rdd1.foreach(x => println(x))
    println("rdd1", rdd1.partitions.length)

    val rdd2 = sc.makeRDD(List(5, 6, 7, 8), 4)
    rdd2.foreach(x => println(x))
    println("rdd2", rdd2.partitions.length)

    //makerdd支持设置数据分片分布的机器
    val rdd3 = sc.makeRDD(List(
      (1, List("iteblog.com", "sparkhost1.com", "sparkhost2.com")),
      (2, List("iteblog.com", "sparkhost2.com")),
      (3, List("iteblog.com", "sparkhost2.com"))
    )
    )
    rdd3.foreach(x => println(x))
    println("rdd3", rdd3.partitions.length)
    println("分区1的位置", rdd3.preferredLocations(rdd3.partitions(0)))
    println("分区2的位置", rdd3.preferredLocations(rdd3.partitions(1)))
    println("分区3的位置", rdd3.preferredLocations(rdd3.partitions(2)))


    //通过读取外部文件生成rdd
    //通过sparkcontext入口处理数据文件
    //默认分区2个
    val rdd4 = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\data\\text\\pdate=2018-04-10").cache()
    //    val rdd4 = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\data\\text\\pdate=2018-04-10",4).cache()
    //所有分区的每个元素
    val temprdd = rdd4.map(x => {
      val array = x.split(",")
      val name = array(0)
      val age = array(1)
      (name, age)
    })
    temprdd.foreach(println)

    //每个分区处理函数
    rdd4.mapPartitions(x => {
      var arraybuff = new ArrayBuffer[String]()
      for (temp <- x) {
        val array = temp.split(",")
        arraybuff += array(0) + "###" + array(1)
      }
      arraybuff.iterator
    }).foreach(println)

    //foreach action动作算子
    rdd4.foreach(x => {
      val array = x.split(",")
      val name = array(0)
      val age = array(1)
      println(name, age)
    })

    println("rdd4", rdd4.partitions.length)
    println("rdd4", rdd4.preferredLocations(rdd4.partitions(0)))
    println("rdd4", rdd4.dependencies(0))
    println("temprdd", temprdd.dependencies(0))

    //结构化数据处理spark程序入口
    val sqlContext = new SQLContext(sc)

    val dataframe1 = sqlContext.createDataFrame(List(
      ("name", "ztwu", "123")
    )).show()

    val dataframe2 = sqlContext.createDataFrame(List(
      P("ztwu1", 25), P("ztwu2", 25), P("ztwu3", 25)
    )).show()

    //createDataFrame，支持通过case class获取schema
    //    val dataframe3 = sqlContext.createDataFrame(List(
    //      P2("ztwu1",25),P2("ztwu2",25),P2("ztwu3",25)
    //    )).show()

    val rdd5 = sc.makeRDD(List("ztwu2#25", "ztwu1#25"))
    rdd5.map(x => {
      val array = x.split("#")
      new P3(array(0), array(1).toInt)
    }).foreach(println(_))

    sc.stop()

  }

  //简单地总结起来就是：让编译器帮忙自动生成常用方法！反过来，如果你手动实现了对应的方法和伴生对象，那么就等同于生成了Case Class.
  // 而实际使用过程中，手动实现这些方法是很繁琐和无趣的，使用Case Class就是最好的选择
  case class P(name: String, age: Int)

}

class P3(val name:String, val age:Int) extends Comparable[P3]
  with Serializable {
  override def compareTo(o: P3): Int = {
    return 0
  }

  override def toString: String = {
    return name+":"+age
  }

}

//类参数在指定val/var修饰
//参数会被默认定义为私有变量private,没有生成setter和getter方法
class P2(val name:String, val age:Int) extends Serializable{

  override def toString: String = super.toString

  override def equals(obj: scala.Any): Boolean = super.equals(obj)

  override def hashCode(): Int = super.hashCode()

  override def clone(): AnyRef = super.clone()

}

object  P2 {
  def apply(name: String, age: Int): P2 = {
    new P2(name, age)
  }

  def unapply(arg: P2): Option[(String, Int)] = {
    if(arg!=null){
      Some(arg.name,arg.age)
    }else {
      None
    }
  }
}

