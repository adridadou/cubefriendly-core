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

trait ResultTransformer {
  def transform(result:Iterator[(Vector[String], Vector[String])], args:Map[String, String]):Iterator[(Vector[String], Vector[String])]
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

object ResultTransformer extends FunctionsHolder[ResultTransformer]{

  def registerTop(): Unit = {
    val transform =
      """
        |import scala.util.Try
        |    val from = args("from").toInt
        |    val to = args("to").toInt
        |    val metricIndex = args.get("metricIndex").map(_.toInt).getOrElse(0)
        |
        |    results.map({ case(vector,metrics) => (vector, metrics.map(m => Try(m.toDouble).toOption.getOrElse(Double.NaN)))}).toVector
        |      .sortBy({case(_,metrics) => -metrics(metricIndex)}).slice(from,to).map({case (vector,metrics) => (vector, metrics.map(_.toString))}).toIterator
      """.stripMargin
    register("top", transform)
  }

  private def parse(transform:String) : Tree = {
    val fun =
      s"""
         |() => new org.cubefriendly.reflection.ResultTransformer {
         | def transform(results:Iterator[(Vector[String],Vector[String])], args:Map[String, String]):Iterator[(Vector[String],Vector[String])] = {$transform
          |}}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
  def register(name:String, transform:String):Unit = {
    val func = parse(transform)
    _funcs.put(name,newFunc(func))
  }

  registerTop()

}
