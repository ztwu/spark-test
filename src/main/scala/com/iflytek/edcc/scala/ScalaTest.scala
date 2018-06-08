package com.iflytek.edcc.scala

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/12/14
  * Time: 15:01
  * Description
  */

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val test = People1()
    test.test()
    test.apply()

    //通过伴生对象的apply方法实现new对象
    //Unit,表示无值，和其他语言中void等同。用作不返回任何结果的方法的结果类型。Unit只有一个实例值，写成()。
    //Any是所有其他类的超类,类似object
    val test2:Any = People2("ztwu2",20)
    test2 match {
      case People21(name,age) => {
        val data = test2.asInstanceOf[People21]
        println("匹配到："+test2.getClass)
        println("匹配到："+data.name+":"+data.age)
      }
      case People2(name,age) => {
        val data = test2.asInstanceOf[People2]
        println("匹配到："+test2.getClass)
        println("匹配到："+data.name+":"+data.age)
      }
      case _ => {
        println("没有匹配到")
      }
    }

    val test21:Any = People21("ztwu21",21)
    test21 match {
      case People21(name,age) => {
        val data = test21.asInstanceOf[People21]
        println("匹配到："+test21.getClass)
        println("匹配到："+data.name+":"+data.age)
      }
      case People2(name,age) => {
        val data = test21.asInstanceOf[People2]
        println("匹配到："+test21.getClass)
        println("匹配到："+data.name+":"+data.age)
      }
      case _ => {
        println("没有匹配到")
      }
    }

    val test3 = new People3("ztwu3",20)
    println("test3 : "+test3.name+":"+test3.age)

    val test4 = new People4("ztwu4",20)
    println("test4 : "+test4.name+":"+test4.age)

    val test5 = new People5("ztwu5",20)
//    test5.age = 100;
    println("test5 : "+test5.name+":"+test5.age)

    //参数定义为变量setter，才有作用
    test3.name = "ztwu6"
    test3.age = 25
    println("test6 : "+test3.name+":"+test3.age)

  }
}

class People1 extends People11 with People1t{

  override def apply() = {
    super.apply()
    println("class people1 apply")
  }

  override def test() = {
    super.test()
    println("class people1 test")
  }

}
class People11 {

  def test() = {
    println("parent class people11 test")
  }

  def apply() = {
    println("parent class people11 apply")
  }
}

//1.主构造器在类名后,参数会被声明字段,若参数没有使用var或者val声明,则会被声明称私有字段

//2,使用var或者val声明参数属性，如果不是private的，那么它也会默认当成private级别的属性,
// 同时会默认生成setter和getter方法，当调用的时候，会通过age的函数来取得age的值，就是getter方法
//而默认生成的setter方法是 age_是age加下划线方式生成setter方法的
//所以可以通过方法访问类的所有的属性

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

//可以认为unapply方法是apply方法的反向操作，apply方法接受构造参数变成对象，而unapply方法接受一个对象，
class People2(val name : String , val age : Int){

  def apply(name:String, age:Int) = {
    println("class people2 apply")
  }

}
case class People21(name:String, age:Int)

class People3(var name : String , var age : Int) {

}

class People4(val name : String , val age : Int) {

}

//参数会被默认定义为私有变量private,没有生成setter和getter方法
class People5(namep : String , agep : Int) {
  //无参的构造函数
  def this(){
    this("",0)
  }

  //getter方法
  def name = namep
  def age = agep

  //setter方法
  //如果参数不是变量，则其setter无用
  def name_=(aName : String) {
    println("传入参数name："+aName)
    val name = aName
  }
  def age_=(aAge : Int) {
    println("传入参数age："+aAge)
    val age = aAge
  }

}

case class MyCaseClass(){

}

//object下的成员都是静态的 ,若有同名的class,这其作为它的伴生类
//在object中一般可以为伴生类做一些初始化等操作
object People1 {

  def apply(): People1 = {
    println("object people1 new")
    new People1()
  }

}

object People2 {

  def apply(name: String, age: Int): People2 = {
    new People2(name, age)
  }

  def unapply(people : People2): Option[(String, Int)] = {
    if(people == null){
      None
    }else {
      Some(people.name,people.age)
    }
  }

}

trait People1t {
  def test() = {
    println("people trait test")
  }
}
