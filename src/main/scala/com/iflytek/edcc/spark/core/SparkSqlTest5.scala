package com.iflytek.edcc.spark.core

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2017/11/29.
  */

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/11/29
  * Time: 8:39
  * Description
  */

object SparkSqlTest5 {

  def testOperation(sc : SparkContext) = {
    var data1 = sc.parallelize(List((1,List(1)),(2,List(2))))
    var data2 = sc.parallelize(List((1,List(3)),(2,List(4))))
    data1.join(data2)
      .reduceByKey({case(a,b) => {
        (a._1++a._2,b._1++b._2)
      }}).foreach(println)

    data1.join(data2)
      .reduceByKey({case(a,b) => {
        (a._1.++(b._1),a._2.++(b._2))
      }}).foreach(println)

    data1.join(data2)//生成List集合的元组
      .foreach(println)

    var data3 = sc.parallelize(Seq((1,Map(1->1)),(1,Map(2->2))))
    data3
      .reduceByKey({case(a,b) => {
        a ++ b
      }}).foreach(println)

  }

  def testScalaCollection(sc : SparkContext) = {
    val arr=sc.parallelize(Array(("A","A"),("B","B"),("C","C")))

    //因为String也是一种序列，而flatmap里传入的函数返回值可以简单理解为也是序列，因此会把所有字符都展开。
    val data1 = arr.flatMap(x=>{
      val temp = x._1+x._2
      temp
    })
    data1.foreach(println)

    val data2 = arr.map(x=>{
      val temp = x._1+x._2
      temp
    })
    data2.toArray().flatten.foreach(x=>{
      println(x)
    })

    Array("A1","B2","C3").flatten.foreach(println)
    Array("A","a","B","b","C","c").flatten.foreach(x=>{
      println(x)
    })

    val list : List[(String,String)] = List(("name","ztwu2"),("age","24"))
    list.toMap.foreach({case(k,v)=>{
      println(k+"="+v)
    }})

    for(i<-0 to list.length-1) {
      val temp = list(i)
      println(temp)
    }

    val map : Map[String,Int] = Map("name"->12,"age"->24)
    //把map里键值对，转换为一个元组，生成一个list的元组集合
    map.toList.foreach(x=>println(x))
    map.toArray.foreach(println)

  }

  def testReadData(sc : SparkContext) = {
    val rdd = sc.textFile("/project/edu_edcc/ztwu2/data/text")
    rdd.collect().map(x => {
      println(x)
    })

    val sqlContext = new SQLContext(sc)
    val dataframe = sqlContext.read.parquet("/project/edu_edcc/ztwu2/data/parquet")
    dataframe.collect().map(x => {
      println(x)
    })

  }

  def testDataFrame(sc : SparkContext) = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.parallelize(Seq(Row("a",10),Row("a",10),Row("b",20),Row("B",30)))
    val schema = StructType(Array(StructField("id", DataTypes.StringType),StructField("cnt",DataTypes.IntegerType)))
    val df = sqlContext.createDataFrame(rdd,schema)
    df.map(x => {
      val id = x.get(0)
      val id2 = x.getAs[String]("id")
      (id,id2)
    }).foreach(println)

    println("test1:---------------------------")
    //dataframe
    df.groupBy("id").count().show()

    println("test2:---------------------------")
    //rdd1
    df.map(x => {
      val id = x(0)
      (id)
    }).groupBy(_.toString)
      .map(x=>(x._1,x._2.size))
      .foreach(println)
    //rdd2
    println(df.map(x => {
      val id = x(0)
      (id)
    }).groupBy(_.toString)
      .count())

    println("test3:---------------------------")
    //dataframe
    df.groupBy("id").agg(("cnt","sum")).show()

    println("test4:---------------------------")
    //dataframe
    df.groupBy("id").sum("cnt").show()

    println("test5:---------------------------")
    //dataframe
    df.groupBy("id").agg(("cnt","max")).where("id = 'a'").select("id").show()

    println("test6:---------------------------")
    //dataframe ===
    df.groupBy("id").agg(("cnt","max")).where($"id"==="B").select($"id").show()

    println("test7:---------------------------")
    //dataframe ===
    df.groupBy("id").agg(("cnt","max")).filter($"id"==="b").select($"id").show()

    //结构化的Rdd
    //没有设置schema,则列字段自动生成为_1,_2
    val dataframe1 = sqlContext.createDataFrame(Seq(("ztwu2",24),("test",20)))
    dataframe1.show()

    //给已有的rdd设置schema来创建dataframe
    val shema2 = StructType(Array(StructField("name1",StringType),StructField("age1",IntegerType)))
    val rdd2 = sc.parallelize(Seq(Row("ztwu2",34),Row("test",30)))
    val dataframe2 = sqlContext.createDataFrame(rdd2,shema2)
    dataframe2.show()
    dataframe2.printSchema()

    //Row(每行记录)强类型的结构化的Rdd
    val datasets1 = sqlContext.createDataset(Seq(People("ztwu2",24),People("test",20)))
    datasets1.show()

    val datasets2 = df.as[Test]
    datasets2.show()

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    conf.set("spark.hadoop.validateOutputSpecs","false")
    conf.setMaster("local[4]")

    val sc = new SparkContext(conf)

    //    testScalaCollection(sc)

    //    testReadData(sc)

        testDataFrame(sc)

//    testOperation(sc)

    sc.stop()

  }

}

//主构造函数
case class People(name : String, age : Int) {

  //构造函数
  def this() {
    this("",0)
  }

}

//默认是可以序列化的，也就是实现了Serializable
//当一个类被声名为case class的时候，scala会帮助我们做下面几件事情：
//1 构造器中的参数如果不被声明为var的话，它默认的话是val类型的，但一般不推荐将构造器中的参数声明为var
//2 自动创建伴生对象，同时在里面给我们实现子apply方法，使得我们在使用的时候可以不直接显示地new对象
//3 伴生对象中同样会帮我们实现unapply方法，从而可以将case class应用于模式匹配，关于unapply方法我们在后面的“提取器”那一节会重点讲解
//4 实现自己的toString、hashCode、copy、equals方法
//除此之此，case class与其它普通的scala类没有区别

//apply方法

//通常，在一个类的半生对象中定义apply方法，在生成这个类的对象时，就省去了new关键字。

//unapply方法

//可以认为unapply方法是apply方法的反向操作，apply方法接受构造参数变成对象，而unapply方法接受一个对象，从中提取值。

case class Test(id:String ,cnt:Int) {

}

//class Test extends Serializable {
//  var id = ""
//  var cnt = 0
//
//  def this(id:String, cnt:Int){
//    this()
//    println(id+" ---- "+cnt)
//    this.id = id
//    this.cnt = cnt
//  }
//}
//
//object Test {
//  def apply(id:String, cnt:Int): Test = {
//    new Test(id,cnt)
//  }
//
//  def unapply(arg: Test): Option[(String,Int)] = {
//    if(arg==null){
//      None
//    }else {
//      Some(arg.id,arg.cnt)
//    }
//  }
//}
