package org.cubefriendly.reflection

import org.cubefriendly.CubefriendlyException

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
  val _funcs:mutable.HashMap[String,() => T] = mutable.HashMap()

  def newFunc(tree:Tree):() => T = Reflection.tb.eval(tree).asInstanceOf[() => T]

  def funcs(name:String) : T = {
    _funcs.get(name) match {
      case Some(f) => f()
      case None => throw new CubefriendlyException("no function " + name + " found. available:" + _funcs.keys.toSeq)
    }
  }

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
    val func = parse(select)
    _funcs.put(name,newFunc(func))
  }
}

object Aggregator extends FunctionsHolder[Aggregator]{

  def registerSum():Unit = {
    val init = "0D"
    val finish = "state.toString"
    val reduce = "{state += value.toDouble\nthis}"

    Aggregator.register("sum",init,reduce,finish)
  }

  def register(name:String, init:String,reduce:String, finish:String):Unit = {
    val func = parse(init,reduce,finish)
    _funcs.put(name,newFunc(func))
  }

  def parse(init:String,reduce:String, finish:String) : Tree = {
    val fun =
      s""" () => new org.cubefriendly.reflection.Aggregator {
         | var state:java.lang.Double = $init
          | def finish:String = $finish
          | def reduce(value:String):org.cubefriendly.reflection.Aggregator = $reduce
          |}
      """.stripMargin

    Reflection.tb.parse(fun)
  }

  registerSum()
}
