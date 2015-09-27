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
  def finish:Double
}

trait DimensionValuesSelector {
  def select(value:String, args: Map[String,String]):Boolean
  def bestResult(value:String, args:Map[String,String]):Boolean
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
    Reflection.tb.eval(tree).asInstanceOf[() => T]()
  }
}

object DimensionValuesSelector extends FunctionsHolder[DimensionValuesSelector]{

  def registerSearch(): Unit = {
    val select = """value.contains(args("term"))"""
    val bestResult = """value.equalsIgnoreCase(args("term"))"""
    register("search", select, bestResult)
  }

  private def parse(select:String, bestResult:String) : Tree = {
    val fun =
      s"""
         |() => new org.cubefriendly.reflection.DimensionValuesSelector {
          | def select(value:String, args:Map[String, String]):Boolean = {$select
          |}
          |def bestResult(value:String, args:Map[String, String]):Boolean = {$bestResult
          |}}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
  def register(name:String, select:String, bestResult:String):Unit = {
    val func = parse(select, bestResult)
    _funcs.put(name,newFunc(func))
  }

  registerSearch()

}


object Aggregator extends FunctionsHolder[Aggregator]{

  def registerSum():Unit = {
    val init = "0D"
    val finish = "state"
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
          | def finish:Double = $finish
          | def reduce(value:String):org.cubefriendly.reflection.Aggregator = $reduce
          |}
      """.stripMargin

    Reflection.tb.parse(fun)
  }

  registerSum()
}
