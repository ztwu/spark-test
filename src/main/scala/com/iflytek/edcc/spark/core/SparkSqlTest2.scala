package com.iflytek.edcc.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/12/13
  * Time: 10:24
  * Description
  */

//spark算子demo
object SparkSqlTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc =new SparkContext(conf)
    val data = sc.makeRDD(Seq(1,2,3))

    //map和flatMap
    data.map(x => {
      println("test-1 : "+x)
    }).collect()

    data.map(x=>fun(x)).collect()

    data.flatMap(x => {
      println("test-3 : "+x)
      1 to x
    }).foreach(println)

    val data1 = sc.makeRDD(Seq("1#1A","2#2A","3#3A"))
    data1.flatMap(x => {
      x.split("#")
    }).foreach(println)

    val tuple = ((1,"A"),(2,"B"))
    val tuple2 = (1,"A",2,"B")
    //productIterator遍历元组的元素
    tuple.productIterator.foreach(println)
    tuple2.productIterator.foreach(println)

    //reduceByKey和reduce
    val test1 = sc.makeRDD(Seq((1,"A"),(2,"A")))
    val test2 = sc.makeRDD(Seq((1,"B"),(2,"B")))
    test1.join(test2)
      .reduce((x,y) => {
        (x._1+y._1,(x._2._1+y._2._1,x._2._2+y._2._2))
      }).productIterator
      .foreach(println)

    test1.join(test2)
      .reduceByKey((x,y) => {
        (x._1,x._2)
      })
      .foreach(println)

    test1.join(test2)
      .flatMap(x => {
        val d1 = x._1
        val d2 = x._2._1
        val d3 = x._2._2
        Array((d1,d2),(d1,d3))
      })
      //reduceByKey返回值的类型和RDD中value的类型一致
      .reduceByKey((x,y)=>{
        x+y
      }).
      foreach(println)

    test1.join(test2)
      .flatMap(x => {
        val d1 = x._1
        val d2 = x._2._1
        val d3 = x._2._2
        Array((d1,d2),(d1,d3))
      })
        .reduce((a,b)=>{
          (a._1+b._1,a._2+b._2)
        }).productIterator
      .foreach(println)

    //groupByKey和groupBy
    println("groupByKey和groupBy算子demo:")
    val da1 = sc.parallelize(Seq((1,20),(1,30),(2,25)))
    val da2 = sc.parallelize(Seq(1,2,3,4,5,6,7))
    //scala模式匹配，元组
    //scala元组Tuple1到Tuple22,1到22为支持的元组最大长度
    da1.groupBy( {
        case(x,y) => {
          if (y % 2 == 0) {
            "a"
          } else {
            "b"
          }
      }
    })
      .foreach(println)
    //groupbykey算子会依据key把value组成集合
    da1.groupByKey().foreach(println)

    //flatMapValues和mapValues
    da1.map(x => {
      (x._1,x._2+"_")
    }).foreach(println)
    da1.mapValues(x=>{x+"_"}).foreach(println)

    da1.flatMap(x=>{
      Array((x._1,x._2+"*"),(x._1,x._2+"_"))
    }).foreach(println)

    da1.flatMapValues(x=>{
      Array(x+"*",x+"_")
    }).foreach(println)

    //mapPartitions
    println("mapPartitions算子测试：")
    val test6 = sc.parallelize(List(1,2,3,4,5,6),2)
    val test7 = sc.makeRDD(List(1,2,3,4,5,6),2)
    test6.mapPartitions(partition => {
      Iterator(fun(partition))
    })
      .foreach(println)

    test6.mapPartitions(partition => {
      var result = List[String]()
      while (partition.hasNext){
        val sum = partition.next() + 1
        result.::(sum)
      }
      result.iterator
    })
      .foreach(println)

    //mapPartitionsWithIndex,partition index
    println("mapPartitionsWithIndex算子测试：")
    test7.mapPartitionsWithIndex((x, partition) => {
      var result = List[String]()
      var sum = 0
      partition.foreach(x=>{
        sum += x
      })
      result.::(x+"|"+sum).iterator
    })
      .foreach(println)

    //mapWith,partition index
    //都是接收两个函数，一个函数把partitionIndex作为输入，输出是一个新类型A；另外一个函数是以二元组（T,A）作为输入，输出为一个序列，这些序列里面的元素组成了新的RDD
    test6.mapWith(a=> {

    })((b,a)=>{

    })


    //unzip
    val zip1 = List((1,2),(1,3),(1,4))
    val result = zip1.unzip
    result.productIterator.foreach(println)

    // aggregateByKey和aggregate
    println("aggregateByKey和aggregate的操作")
    test1.join(test2)
      .flatMap(x => {
        val d1 = x._1
        val d2 = x._2._1
        val d3 = x._2._2
        Array((d1,d2),(d1,d3))
      }).repartition(2)
      //aggregateByKey返回值的类型不需要和RDD中value的类型一致
      .aggregateByKey(new HashSet[String])(
      //seqOp:用来在同一个partition中合并值
      (a , b) =>{
        println("aggregateByKey-step1: : "+(a.+(b)))
        a+b
      },
      //combOp:用来在不同partiton中合并值
      (a , b)  => {
        println("aggregateByKey-step2: : "+(a++b))
        a.++(b)
      })
      .foreach(println)

    println("split:----------------------------------------------------------")

    test1.join(test2)
      .flatMap(x => {
        val d1 = x._1
        val d2 = x._2._1
        val d3 = x._2._2
        Array((d1,d2),(d1,d3))
      })
      .aggregate(new HashSet[(Int, String)])(
        (a,b) => {
          println("aggregate-step1: : "+(a.+(b)))
          a.+(b)
        },
        (p1,p2) => {
          println("aggregate-step2: : "+(p1++p2))
          p1 ++ p2
        }
      ).foreach(println)

    //combineByKey
    //1.6.0版的函数名更新为combineByKeyWithClassTag
    //createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)
    // 并把它返回 (这一步类似于初始化操作),createCombiner就是定义了v如何转换为c

//    mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上
    // (这个操作在每个分区内进行)

//    mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      //createCombiner,初始化操作
      score => (1, score),
      //mergeValue
      (c1: MVType, newScore) => {
        println("step1: "+(c1._1 + 1, c1._2 + newScore))
        (c1._1 + 1, c1._2 + newScore)
      },
      //mergeCombiners
      (c1: MVType, c2: MVType) => {
        println("step2: "+(c1._1 + c2._1, c1._2 + c2._2))
        (c1._1 + c2._1, c1._2 + c2._2)
      }
    ).map {
        case (name, (num, socre)) => {
          (name, socre / num)
        }
    }.foreach(println)

    //option选项集合
    val test21 = Option(10)
    val test22 = Option(null)
    val test23 = Option("")
    val test24 = Option(" ")

    val test25 = Some(50)
    val test26 = None

    val test27:Option[String] = Some("hello")
    val test28:Option[String] = None

    println(test21.get)
    println(test22.getOrElse(20))
    println(test23.getOrElse(21))
    println(test24.getOrElse(22))

    println(test25.get)
    println(test26.getOrElse(100))

    println(test27.get)
    println(test28)

  }

  //seqOp:用来在同一个partition中合并值
  def seqOp(a:String, b:String) : String = {
    math.min(a.length , b.length ).toString
  }

  //combOp:用来在不同partiton中合并值
  def comOp(a:String, b:String) : String = {
    a + b
  }

  def fun(x : Int) = {
    println("test-2 : "+x)
  }

  def fun(partition :Iterator[Int]): Int = {
    var total = 0
    partition.foreach(x => {
      total += x
    })
    total
  }

}
