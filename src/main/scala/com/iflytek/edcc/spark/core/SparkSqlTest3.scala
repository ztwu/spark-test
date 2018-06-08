package com.iflytek.edcc.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/12/13
  * Time: 15:36
  * Description
  */

object SparkSqlTest3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)

    //SparkContext创建TaskSchedulerImpl，SparkDeploySchedulerBackend和DAGScheduler
    //DAGScheduler负责将Job划分为不同的Stage，并在每个Stage内化为出一系列可并行处理的task
    //然后将task递交给TaskSchedulerImpl调度
    //TaskSchedulerImpl负责通过SparkDeploySchedulerBackend来调度任务（task），
    // 目前实现了FIFO调度和Fair调度。注意如果是Yarn模式，则是通过YarnSchedulerBackend来进行调度。
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\data\\text");
    rdd1.foreach(println)
    println("rdd1 : "+rdd1.getNumPartitions)
    val data1 = rdd1.map(x=>{
      val line = x.split(",")
      val name = line(0)
      val age = line(1)
      (name,age)
    })
    println("data1 : "+data1.getNumPartitions)

    val rdd2 = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\data\\text2");
    rdd2.foreach(println)
    println("rdd2 : "+rdd2.getNumPartitions)
    val data2 = rdd2.map(x=>{
      val line = x.split(",")
      val name = line(0)
      val city = line(1)
      (name, city)
    })
    println("data2 : "+data2.getNumPartitions)

    val res  = data1.join(data2)
    println("res : "+res.getNumPartitions)

    res.foreach(println)

    //因为checkpoint会触发一个Job,如果执行checkpoint的rdd是由其他rdd经过许多计算转换过来的，
    // 如果你没有persisted这个rdd，那么又要重头开始计算该rdd，也就是做了重复的计算工作了，
    // 所以建议先persist rdd然后再checkpoint，checkpoint会丢弃该rdd的以前的依赖关系，
    // 使该rdd成为顶层父rdd，这样在失败的时候恢复只需要恢复该rdd,而不需要重新计算该rdd了，
    // 这在迭代计算中是很有用的，假设你在迭代1000次的计算中在第999次失败了，然后你没有checkpoint，你只能重新开始恢复了，
    // 如果恰好你在第998次迭代的时候你做了一个checkpoint，那么你只需要恢复第998次产生的rdd,
    // 然后再执行2次迭代完成总共1000的迭代，这样效率就很高，比较适用于迭代计算非常复杂的情况，
    // 也就是恢复计算代价非常高的情况，适当进行checkpoint会有很大的好处。
    sc.setCheckpointDir("D:\\project\\edu_edcc\\ztwu2\\checkpoint")
    //个函数还为数据提供了位置信息
    //其中作者也说明了,在checkpoint的时候强烈建议先进行cache,
    // 并且当你checkpoint执行成功了,那么前面所有的RDD依赖都会被销毁,如下:
    var rdd = sc.makeRDD(List(1,2,3,4,5,6))
      .map(x=>{
      println("打印下每个值："+x)
      x
    })

    rdd.cache()
    rdd.checkpoint()
    //checkpoint是会单独另起一个job去重新计算rdd,第一次可以cache起来
//    rdd.cache()
//    rdd.persist(StorageLevel.DISK_ONLY)

    println(rdd.collect())
    println(rdd.collect())

//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val dataframe = sqlContext.read.parquet("D:\\project\\edu_edcc\\ztwu2\\data\\parquet")
//    dataframe.show()

  }

}
