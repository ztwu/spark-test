package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/4/16
  * Time: 9:52
  * Description 扩展Spark API，
  * （1）、其中一种就是在现有的RDD中添加自定义的方法；
  * （2）、第二种就是创建属于我们自己的RDD
  */

object SparkRddTest1 {

  def main(arg:Array[String]):Unit = {

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)

//    val rdd = sc.makeRDD(List(
//      new User("ztwu1",25),
//      new User("ztwu2",25),
//      new User("ztwu3",25)
//    ))
//      .foreach(println(_))

    //隐式转换通过object伴生对象
    val rdd1 = sc.makeRDD(List(
      ("01","01","01",10),
      ("02","02","02",20),
      ("03","03","03",30),
      ("04","04","04",40)
    ))

    val rdd2 = rdd1.map(x=>{
      new SalesRecord(x._1,x._2,x._3,x._4)
    })

    val rdd3 = sc.makeRDD(List(
      1,2,3,4,5
    ))

    import ImplicitObjectTest._
    println(1.add)

//    import IteblogCustomFunctionsIntRdd._
//    println(rdd3.totalSales)
//
//    import IteblogCustomFunctions._
//    println(rdd2.totalSales)

  }

}

case class User(name:String, age:Int)

class SalesRecord(val id: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double) extends Serializable

//class IteblogCustomFunctions(rdd:RDD[SalesRecord]) {
//  def totalSales = rdd.map(_.itemValue).sum
//}
//
//object IteblogCustomFunctions {
//  implicit def addIteblogCustomFunctions(rdd: RDD[SalesRecord]) = {
//    println("类型隐式转换")
//    new IteblogCustomFunctions(rdd)
//  }
//}

object IteblogCustomFunctions {
  implicit class IteblogCustomFunctions(rdd:RDD[SalesRecord]) {
    def totalSales = rdd.map(_.itemValue).sum
  }
}

object IteblogCustomFunctionsIntRdd {
  implicit class IteblogCustomFunctionsIntRdd(rdd:RDD[Int]) {
    def totalSales = rdd.map(x=>{x}).sum()
  }
}

object ImplicitObjectTest{
  implicit class testObject(p:Int){
    def add = p+10
  }
}