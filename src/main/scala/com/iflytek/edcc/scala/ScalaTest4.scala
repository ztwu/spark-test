package com.iflytek.edcc.scala

import akka.actor._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/3/28
  * Time: 15:04
  * Description
  */
case object PingMessage
case object PongMessage
case object StartMessage
case object StopMessage

/**
*  scala通过actor模型实现的多并发
  *
  *  Akka 是一个用 Scala 编写的库，用于简化编写容错的、高可伸缩性的 Java 和 Scala 的 Actor 模型应用。
  *
  *  ! 发送异步消息，没有返回值。
    !? 发送同步消息，等待返回值。
    !! 发送异步消息，返回值是 Future[Any]。
  *
  */
object ScalaTest4{
  def main(args: Array[String]): Unit = {

    val system = ActorSystem("pingpongsytem")

    val pong = system.actorOf(Props[Pong],name="pong")

    val ping = system.actorOf(Props(new Ping(pong)),name="ping")

    /**
    *   避免使用同步调用（!?），它们会阻塞，很有可能引发死锁。
      */
    ping ! StartMessage
  }
}

class Ping(pong:ActorRef) extends Actor{

  var count= 0

  def incrementAndPrint() = {
    count+=1;
    println("ping")
  }

  override def receive: Receive = {
    case StartMessage =>
      incrementAndPrint()
      pong ! PingMessage

    case PongMessage =>
      incrementAndPrint
      if(count>99){
        //Actor可以返回消息给发送方。Receive会把sender字段设为当前消息的发送方。
        sender ! StopMessage
        println("ping stopped")
        context.stop(self)
      }else{
        //Actor可以返回消息给发送方。Receive会把sender字段设为当前消息的发送方。
        sender ! PingMessage
      }

    case _ =>println("Ping got something unexpected")
  }

}
class Pong extends Actor{

  override def receive: Receive = {
    case PingMessage =>
      println("pong")
      //Actor可以返回消息给发送方。Receive会把sender字段设为当前消息的发送方。
      sender ! PongMessage

    case StopMessage =>
      println("pong stopped")
      context.stop(self)

    case _ =>println("pong got something unexpected")
  }

}