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

object ResultTransformer extends FunctionsHolder[ResultTransformer]{

  def registerTop(): Unit = {
    val transform =
      """
        |import scala.util.Try
        |    val max = args("limit").toInt
        |    val metricIndex = args.get("metricIndex").map(_.toInt).getOrElse(0)
        |    var minValue = Double.NaN
        |    results.foldLeft(scala.collection.mutable.Buffer[(Vector[String], Vector[String])]())((buffer,record) => {
        |      if(buffer.length < max){
        |        buffer.append(record)
        |        minValue = buffer.map({case (vector,metrics) => Try(metrics(metricIndex).toDouble).toOption.getOrElse(Double.NaN)}).min
        |      }else {
        |        val currentValue = Try(record._2(metricIndex).toDouble).toOption.getOrElse(Double.NaN)
        |        if(minValue < currentValue) {
        |          val index = buffer.indexWhere({case(vector, metrics) => Try(metrics(metricIndex).toDouble).toOption.getOrElse(Double.NaN) == minValue})
        |          buffer.remove(index)
        |          buffer.append(record)
        |          minValue = buffer.map({case (vector,metrics) => Try(metrics(metricIndex).toDouble).toOption.getOrElse(Double.NaN)}).min
        |        }
        |      }
        |
        |      buffer
        |    }).sortBy({case(vector,metrics) => -Try(metrics(metricIndex).toDouble).toOption.getOrElse(Double.NaN)}).toIterator
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
