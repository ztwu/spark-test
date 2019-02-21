package com.iflytek.edcc.scala

/**
  * created with idea
  * user:ztwu
  * date:2019/2/21
  * description
  */
object ScalaTest6 {

  def main(args:Array[String]):Unit = {
    val f = (x:Int) => {x+1}
    val a = Array(1,2)
    test(f)
    a.foreach((x:Int) => {
      println(x)
      x+1
    })

    val l = List(1,2,"a")
    l.foreach({case (x) => {println(x)}})

    val o:Any = 3;
    o match {
      case x:Int=> println("int")
      case y:String=> println("string")
      case _ => println("else")
    }
  }

  def test(x:Int=>Int): Unit = {
    val r = x(2)
    println(r)
  }

}
