package com.iflytek.edcc.rdd

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

/**
  * Created with Intellij IDEA.
  * User2: ztwu2
  * Date: 2018/4/16
  * Time: 15:18
  * Description 自定义RDD
  */

object sparkRddTest2 {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      User2("ztwu1",25),
      User2("ztwu2",26),
      User2("ztwu3",27)
    ))

    val rdd1 = new MyRdd(rdd,10)
    println(rdd1.collect().toBuffer)

  }

}

case class User2(name:String, age:Int){

}

class MyRdd(prv:RDD[User2],ageParam:Int) extends RDD[User2](prv:RDD[User2]) {

  //这个函数是用来计算RDD中每个的分区的数据
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[User2] = {
    firstParent[User2].iterator(split, context).map(user => {
      val age = user.age-ageParam
      new User2(user.name, age)
    })
  }

  //获取分区，getPartitions函数允许开发者为RDD定义新的分区，在我们的代码中，并没有改变RDD的分区，重用了父RDD的分区。
  override protected def getPartitions: Array[Partition] = {
    firstParent[User2].partitions
  }
}

//scala泛型
class Test[T] {
  def test(t:T) = {

  }
}