package com.iflytek.edcc.scala

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/15
  * Time: 14:35
  * Description
  */

//scala的隐式转换
//掌握implicit的用法是阅读spark源码的基础，也是学习scala其它的开源框架的关键，implicit 可分为：

//隐式参数
//隐式转换类型
//隐式调用函数

//implicit关键字只能用来修饰方法、变量（参数)和伴随对象。

object ScalaimplicitTest {

  //1.隐式参数
  /**
  *   当我们在定义方法时，可以把最后一个参数列表标记为implicit，表示该组参数是隐式参数。
    *   一个方法只会有一个隐式参数列表，置于方法的最后一个参数列表。
    *   如果方法有多个隐式参数，只需一个implicit修饰即可。 当调用包含隐式参数的方法是，
    *   如果当前上下文中有合适的隐式值，则编译器会自动为改组参数填充合适的值。
    *   如果没有编译器会抛出异常。当然，标记为隐式参数的我们也可以手动为该参数添加默认值。
    *   def foo(n: Int)(implicit t1: String, t2: Double = 3.14)
    */
  def calcTax(amount: Float)(implicit rate: Float): Float = {
    amount * rate
  }

  //2.隐式地转换类型,隐式函数
  //隐式函数是在一个scop下面，给定一种输入参数类型，自动转换为返回值类型的函数，和函数名，参数名无关。
  implicit def doubleToIntTest(dParam: Double) = {
    dParam.toInt
  }

  implicit val test = 10;

  def main(args: Array[String]): Unit = {

    //test1
    implicit val currentTaxRate = 0.08F
    val tax = calcTax(50000F) // 4000.0
    println(tax)

    //test2
    val i: Int = 3.5  //i=3
    println(i)

    println(test)

    import com.iflytek.edcc.scala.ImplicitObjectTest._
    println(1.add(2,2))

    val temp = AminalType
    temp.test()
    temp.test2()

//    implicit object testObject extends testClass[Int]{
//      override def add2(x: Int, y: Int) : Int = x + y
//      override def unit: Int = 0
//    }
//
//    def sum[T](xs: List[T])(implicit m: testClass[T]): T = {
//      if(xs.isEmpty){
//        m.unit
//      }else {
//        m.add2(xs.head, sum(xs.tail))
//      }
//    }
//    println(sum(List(1,2,3,4,5)))

  }

}

//3.隐式调用函数
/**
*   隐式调用函数可以转换调用方法的对象，比如但编译器看到X .method，
  *   而类型 X 没有定义 method（包括基类)方法，那么编译器就查找作用域内定义的从 X 到其它对象的类型转换，
  *   比如 Y，而类型Y定义了 method 方法，编译器就首先使用隐含类型转换把 X 转换成 Y，
  *   然后调用 Y 的 method。
  */
//写法第一种，implicit class，隐式类
object swimming2{
  //被隐式转换的对象作为参数传入
  implicit class SwingType(t:AminalType) {
    def wantLearned(sw : String) = {
      println("测试2"+sw)
    }
  }
}

//写法第二种，implicit method，隐式方法
class SwingType{
  def  wantLearned(sw : String) = {
    println("测试"+sw)
  }
}
object swimming{
  //被隐式转换的对象作为参数传入
  implicit def learningType(s : AminalType) = {
    new SwingType
  }
}

class AminalType {

}
object AminalType extends App {

  def test(): Unit ={
    //引入，即为加入到该类作用域下
    import com.iflytek.edcc.scala.swimming._
    val rabbit = new AminalType
    rabbit.wantLearned("breaststroke")
  }

  def test2() = {
    import com.iflytek.edcc.scala.swimming2._
    val rabbit = new AminalType
    rabbit.wantLearned("breaststroke2")
  }

}

object ImplicitObjectTest{
  implicit class testObject(p:Int){
    def add(x: Int, y: Int) = x + y + p
  }
}

abstract class testClass[T] {
  def add2(x:T, y:T) : T
  def unit : T
}