package com.iflytek.edcc.spark.core

/**
  * Created by ztwu2 on 2017/10/11.
  */
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlTest1 {

  val logger = Logger.getLogger(SparkSqlTest1.getClass)

  val sparkConf = new SparkConf().setAppName("SparkSQL")
  //设置数据自动覆盖
  sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
  sparkConf.setMaster("local[4]")
  val sc = new SparkContext(sparkConf)

  // 样例类
  case class People(name : String, age : Int)

  //dataFrame测试
  def testDataFrame(sc : SparkContext) {

    val sqlContext = new SQLContext(sc)
    // 为了支持RDD到DataFrame的隐式转换 toDF()
    import sqlContext.implicits._

//    val mobiles = sqlContext.createDataFrame(Seq((1,"Android"), (2, "iPhone")))

    val mobiles = sqlContext.createDataFrame(Seq(People("ztwu1",24),People("ztwu2",24)))
    mobiles.printSchema()
    mobiles.show()

    mobiles.filter(mobiles("name") === "ztwu2").show()

    //创建spark临时表,以便使用sql语句
    mobiles.registerTempTable("test")
    val name = "ztwu1"
    val age = sqlContext.sql(s"select age from test where name = '${name}'").first().getInt(0)
    println(age)

    logger.info(sqlContext.sql(s"select * from test where name = '${name}'").first())

  }

  //rdd测试
  def testRdd(sc : SparkContext) {

    val rdd = sc.makeRDD(Seq(People("ztwu1",24),People("ztwu2",24)))
      .collect()
      .map(x => {
        val name = x.name
        val age = x.age
        (name,age)
      })
      .filter(_._1 == "ztwu1")
      .foreach(println)

  }

  //通过sqlContext读取parquet文件
  def getLunarDate (sc : SparkContext, day : String): String = {

    val sqlContext = new SQLContext(sc)
    // 为了支持RDD到DataFrame的隐式转换 toDF()

    //DataFrame(SchemaRdd)是分布式的Row对象的集合
    val dataFrame = sqlContext.read.parquet("/project/zx_dev/zx_dev/db/util_date_relation/")
    //注册临时表
    //registerTempTable是将表名(或表的标识)和对应的逻辑计划加载到Map
    dataFrame.registerTempTable("util_date_relation")
    sqlContext.sql(
      "select\n" +
        "lunar_day\n" +
        "from util_date_relation\n" +
        s"where day = '${day}'").first().getString(0)

  }

  //通过sparkContext读取txt文件
  def getLunarDateByRdd (sc : SparkContext, day : String): String = {

    sc.textFile("/project/zx_dev/zx_dev/db/util_date_relation/")
      .map(x => {
          val l = x.split("\t")
          val day = l(3)
          val lunar_day = l(9)
          (day,lunar_day)
      })
      .filter(_._1 == day)
      .first()._2
  }

  //dataframe join可以指定2个dataframe的join关联的字段
  def testJoin(sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    // 为了支持RDD到DataFrame的隐式转换 toDF()
    import sqlContext.implicits._

    val data = List(People("ztwu1",24),People("ztwu1",34))

    val mobiles1 = sqlContext.createDataFrame(data)

    //    mobiles1.show()

    val t1 = mobiles1.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    })
      .toDF("name","age")

    //    t1.show()

    val mobiles2 = sqlContext.createDataFrame(Seq(People("ztwu1",28),People("ztwu2",38)))

    val t2 = mobiles2.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    }).toDF("name","age")

    //    t2.show()

    val result = t1.join(t2,t1("name")===t2("name"),"left")

    result.map(x => {
      val name1 = x(0)
      val age1 = x(1)
      val name2 = x(2)
      val age2 = x(3)
      (name1,age1,name2,age2)
    }).foreach(println)

    //    result.show()

  }

  //rdd数据集合join，只返回两个RDD根据K可以关联上的结果，join只能用于两个RDD之间的关联
  def testRDDJoin(sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    // 为了支持RDD到DataFrame的隐式转换 toDF()

    val data = List(People("ztwu1",24),People("ztwu1",34))

    val mobiles1 = sqlContext.createDataFrame(data)

    //    mobiles1.show()

    val t1 = mobiles1.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    })

    //    t1.show()

    val mobiles2 = sqlContext.createDataFrame(Seq(People("ztwu1",28),People("ztwu2",38)))

    val t2 = mobiles2.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    })

    //    t2.show()

    val result = t1.join(t2)

    result.map(x => {
      val name1 = x._1
      val age1 = x._2._1
      val name2 = x._1
      val age2 = x._2._2
      (name1,age1,name2,age2)
    }).foreach(println)

    //    result.show()

  }

  //ReduceByKey功能，聚合函数自定义,min()
  def testReduceByKey(sc : SparkContext) = {
    val rdd = sc.parallelize(Array((1,4),(1,5),(2,7),(2,6)))
      .reduceByKey({case (a, b) => {
        if(a < b){
          a
        }else {
          b
        }
      }})
    println(rdd.collect().toBuffer)
  }

  def testReduceByKey2(sc : SparkContext) = {
    val rdd = sc.parallelize(Array((1,4),(1,5),(2,7),(2,6))).reduceByKey({_+_})
    println(rdd.collect().toBuffer)

    val rdd2 = sc.parallelize(Array((1,4),(1,4),(2,7),(2,6))).reduce({(a,b) => (a._1,b._2+a._2)})
    println(rdd2)
  }

  //reduce
  def testReduce(sc : SparkContext) = {

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val rdd = sc.parallelize(Seq((1,1),(1,2),(1,3)))
        .reduce((a,b) => (a._1 + b._1, a._2 + b._2))
    println(rdd)

    val rddd = sc.parallelize(Array(1,2,3,4))
      .reduce({case (a,b) => {(a+b)}})
    println(rddd)

    val dataframe = sc.makeRDD(1 to 10).toDF("user_no")
    dataframe.printSchema()
    dataframe.show()

  }

  //scala数据集合
  def testSeq(sc : SparkContext) = {

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val array = Array(10,11,12)

    val list = 1 to 10
    val set = Set(1 to 10)
    val map = Map("id"->1,"name"->"ztwu2")
    val tuple = ("01",12)
    val iterator = Iterator("Baidu", "Google", "Runoob", "Taobao")

    val dataframe = sc.makeRDD(array).toDF("user_no")
    dataframe.printSchema()
    dataframe.show()

//    list.foreach(println)

    for(item <- array){
      println(item)
    }

    for(item <- list){
      println(item)
    }

    for(item <- map){
      println(item._1+"----"+item._2)
    }

    while (iterator.hasNext){
      println(iterator.next())
    }

  }

  //读取数据
  def testReadData(sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val text = sqlContext.read.text("/project/edu_edcc/ztwu2/data/text").as[String]
    val json = sqlContext.read.json("/project/edu_edcc/ztwu2/data/json")
    val parquet = sqlContext.read.parquet("/project/edu_edcc/ztwu2/data/parquet")

    //创建Properties存储数据库相关属性
//    val prop = new Properties()
//    prop.put("user", "edmpbi")
//    prop.put("password", "Bi@edmp")
//    val jdbc = sqlContext.read.jdbc("jdbc:mysql://192.168.59.214:3306/edmp_bi?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull","bi_area",prop)

//    json.show()
//    parquet.show()
//    jdbc.show()

    parquet.filter("name = 'Ben'").show()

    val test = parquet.map(x => {
      val name = if (x(0) == null || x(0) == "") "bg-1" else x(0).toString
      val color = if (x(1) == null || x(1) == "") "bg-1" else x(1).toString
      (name, color)
    })
//    test.toDF("name","color")
//          .write.mode(SaveMode.Overwrite).parquet("D:\\project\\edu_edcc\\ztwu2\\temp\\spark-test")

    val test1 = parquet.map(x => {
      val name = x(0)
      val color = x(1)
      (name, color)
    }).foreach(println)

//    text.show()
//    text.printSchema()

//    val idAgeRDDRow = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))
//    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))
//    val idAgeDF = sqlContext.createDataFrame(idAgeRDDRow, schema)
//    idAgeDF.printSchema()

  }

  //dataframe join可以指定2个dataframe的join关联的字段
  def testJoin2(sc : SparkContext) = {
    val sqlContext = new SQLContext(sc)
    // 为了支持RDD到DataFrame的隐式转换 toDF()
    import sqlContext.implicits._

    val data = List(People("ztwu1",24),People("ztwu1",34))

    val mobiles1 = sqlContext.createDataFrame(data)

    //    mobiles1.show()

    val t1 = mobiles1.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    })
      .toDF("name","age")

    //    t1.show()

    val mobiles2 = sqlContext.createDataFrame(Seq(People("ztwu1",28),People("ztwu2",38)))

    val t2 = mobiles2.map(x => {
      val name = x(0).toString
      val age = x(1).toString.toInt
      (name,age)
    }).toDF("name","age")

    //    t2.show()

    val result = t1.join(t2,t1("name")===t2("name"),"left")

    result.show()

    result.map(x => {
      val name1 = x.getAs[String]("name")
      val age1 = x.getAs[Int]("age")
      val name2 = x(2)
      val age2 = x(3)
      (name1,age1,name2,age2)
    }).foreach(println)

    //    result.show()

  }

  def main(args: Array[String]): Unit = {
//        testRdd(sc)
//        testDataFrame(sc)

//        val day = getLunarDate(sc,"2017-10-18")
//        logger.info("---------------------------day---------------------:"+day)
//        val day = getLunarDateByRdd(sc,"2010-01-18")
//        logger.info("---------------------------day---------------------:"+day)

//        testJoin(sc)
//        testRDDJoin(sc)

//    testReduce(sc)
//    testReduceByKey(sc)

//    testSeq(sc)

    testReadData(sc)
//    testReduceByKey2(sc)

//    testJoin2(sc)

    sc.stop()

  }

}
