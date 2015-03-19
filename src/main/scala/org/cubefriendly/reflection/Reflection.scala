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

object Aggregator {

  def registerSum():Unit = {
    val init = "BigDecimal(0)"
    val finish = "state.toString"
    val reduce = "{state += BigDecimal(value)\nthis}"

    Aggregator.register("sum",init,reduce,finish)
  }

  private val funcs:mutable.HashMap[String,Tree] = mutable.HashMap()

  def newFunc[T](name:String):Aggregator = newFunc(funcs(name))
  def register(name:String, init:String,reduce:String, finish:String):Unit = {
    val func = parse(init,reduce,finish)
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

  def newFunc(tree:Tree):Aggregator = Reflection.tb.eval(tree).asInstanceOf[Aggregator]

  def build(init:String,reduce:String, finish:String) : Aggregator = {
    Reflection.tb.eval(parse(init,reduce,finish)).asInstanceOf[Aggregator]
  }
}