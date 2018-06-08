package com.iflytek.edcc.scala

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/3/28
  * Time: 14:42
  * Description
  */

object ScalaTest3 {

  def main(args:Array[String]):Unit = {

    val r1 = new ThreadExam()
    val t1 = new Thread(r1)
    val t2 = new ThreadExam2()
    t1.start()
    t1.join()
    t2.start()
  }

}

/**
* scala通过java线程实现的
  */
class ThreadExam extends Runnable {

  override def run(): Unit = {
    for(i <- 1 to 10){
      println("thread1 is running...."+i+"次")
    }
  }

}
class ThreadExam2 extends Thread {
  override def run(): Unit = {
    for(i <- 1 to 10){
      println("thread2 is running...."+i+"次")
    }
  }
}
