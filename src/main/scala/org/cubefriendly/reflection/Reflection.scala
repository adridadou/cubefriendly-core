package org.cubefriendly.reflection

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/**
 * Cubefriendly
 * Created by david on 12.03.15.
 */

object Reflection {
  lazy val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
}

trait Aggregator {
  def reduce(value:String):Aggregator
  def finish:String
}

trait DimensionValuesSelector {
  def select(args: String*):Vector[String]
}

trait FunctionsHolder[T] {
  val funcs:mutable.HashMap[String,T] = mutable.HashMap()

  def newFunc(tree:Tree):T = Reflection.tb.eval(tree).asInstanceOf[T]


  def build(tree:Tree) : T = {
    Reflection.tb.eval(tree).asInstanceOf[T]
  }
}

object DimensionValuesSelector extends FunctionsHolder[DimensionValuesSelector]{

  private def parse(select:String) : Tree = {
    val fun =
      s"""
         |new org.cubefriendly.reflection.DimensionValuesSelector {
          | def select(args:String*):Vector[String] = $select
          |}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
  def register(name:String, select:String):Unit = {
    val func = newFunc(parse(select))
    funcs.put(name,func)
  }
}

object Aggregator extends FunctionsHolder[Aggregator]{

  def registerSum():Unit = {
    val init = "BigDecimal(0)"
    val finish = "state.toString"
    val reduce = "{state += BigDecimal(value)\nthis}"

    Aggregator.register("sum",init,reduce,finish)
  }

  def register(name:String, init:String,reduce:String, finish:String):Unit = {
    val func = newFunc(parse(init,reduce,finish))
    funcs.put(name,func)
  }

  def parse(init:String,reduce:String, finish:String) : Tree = {
    val fun =
      s"""
         |new org.cubefriendly.reflection.Aggregator {
         | var state = $init
          | def finish:String = $finish
          | def reduce(value:String) = $reduce
          |}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
}
