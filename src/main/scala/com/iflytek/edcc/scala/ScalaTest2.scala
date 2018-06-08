package com.iflytek.edcc.scala

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/3/28
  * Time: 13:53
  * Description
  */

object ScalaTest2 {

  def main(args:Array[String]):Unit = {
//    val demo = new Demo()
    val demo = Demo()
    try {
      demo.test()
    } catch {
      case e:NullPointerException =>{
        println(e.getMessage)
      }
      case e: NumberFormatException => {
        println(e.getMessage)
      }
    } finally {
      println("end")
    }
  }
}

class Demo {
  @throws(classOf[NumberFormatException])
  def test(): Unit ={
    "aaa".toInt
  }

  def test2()={
    println("test2")
  }

}

object Demo {

  def apply(): Demo = {
    println("new demo")
    new Demo()
  }

  def unapply(demo : Demo): Option[(Int,String)] = {
    if(demo == null){
      None
    }else {
      Some(1,"1")
    }
  }
  
}
