package com.iflytek.edcc.scala

import scala.actors.Actor

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/3/28
  * Time: 16:47
  * Description
  */

object ScalaTest5 {
 def main(args:Array[String]):Unit = {
//   val act1 = MyActor1
//   val act2 = MyActor2
//
//   act1.start()
//   act2.start()
//
//   val act22 = MyActor2()
//   act22.test1()
//   act2.act()

   val act = new MyActor()
//   val act = new YourActor()
   act.start()
   act ! "stop"
   act ! "start"

   //发送同步消息
   val context = act !? "stop"
   println(context)

   //异步但是返回结果，这个结果会被放到future中。然后进行返回
//   val context2 = act !! "stop"
//   println(context2)

 }
}

//这种执行完成之后就停止了。
object MyActor1 extends Actor {
  //重新act方法
  def act(): Unit = {
    for(i <- 1 to 10) {
      println("actor-1 " + i)
      Thread.sleep(3000)
    }
  }
}

object MyActor2 extends Actor {

  def act(): Unit = {
    for(i <- 1 to 10) {
      println("actor-2 " + i)
      Thread.sleep(2500)
    }
  }

  def apply(): MyActor2 = {
    new MyActor2()
  }
}

class MyActor2 {
  def test1(): Unit ={
    println("test1")
  }
}

class MyActor extends Actor {

  override def act(): Unit = {
    while(true) {
      //receive相当于是创建线程和销毁线程的过程。
      receive {
        case "start" => {
          println("starting...")
          Thread.sleep(5000)
          println("stared")
        }
        case "stop" => {
          println("stoping ...")
          Thread.sleep(5000)
//          println("stopped ...")
          sender ! "you has stoped!"
        }
      }
    }
  }
}

/**
 *   react 如果要反复执行消息处理，react外层要用loop，不能用while
  */
class YourActor extends Actor {

  override def act(): Unit = {
    loop {
      //这里是一个偏函数
      react {
        case "start" => {
          println("starting...")
          Thread.sleep(5000)
          println("started")
        }
        case "stop" => {
          println("stopping...")
          Thread.sleep(8000)
          println("stoped")
        }
      }
    }
  }
}
