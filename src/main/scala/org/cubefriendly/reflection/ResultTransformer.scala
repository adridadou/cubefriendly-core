package org.cubefriendly.reflection

import scala.reflect.runtime.universe._

/**
 * Cubefriendly
 * Created by davidroon on 04.10.15.
 * This code is released under Apache 2 license
 */
trait ResultTransformer {
  def transform(result:Iterator[(Vector[String], Vector[String])], args:Map[String, String]):Iterator[(Vector[String], Vector[String])]
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
        |    result.map({ case(vector,metrics) => (vector, metrics.map(m => Try(m.toDouble).toOption.getOrElse(Double.NaN)))}).toVector
        |      .sortBy({case(_,metrics) => -metrics(metricIndex)}).slice(from,to).map({case (vector,metrics) => (vector, metrics.map(_.toString))}).toIterator
      """.stripMargin
    register("top", transform)
  }

  def registerSlice(): Unit = {
    val transform =
      """
        |import scala.util.Try
        |    val newResult = result.toVector.sortBy({case (vector,metrics) => -Try(metrics.head.toDouble).toOption.getOrElse(Double.PositiveInfinity)})
        |    val from = Math.max( args("from").toInt, 0)
        |    val to = Math.min(args("to").toInt, newResult.length)
        |    newResult.slice(from, to).toIterator
      """.stripMargin

    register("slice", transform)
  }

  def registerSearchForValue(): Unit = {
    val transform = """
      |import scala.util.Try
      |    val value = args("value")
      |    val dimIndex = args("dimIndex").toInt
      |    val limit = args("limit").toInt
      |    val newResult = result.toVector.sortBy({case (vector,metrics) => -metrics.head.toDouble})
      |    val resultIndex = newResult.indexWhere({case (vector,metrics) => vector(dimIndex) == value})
      |    val from = Math.max( resultIndex - (limit / 2), 0)
      |    val to = Math.min(resultIndex + (limit / 2), newResult.length)
      |    newResult.slice(from, to).toIterator
    """.stripMargin

    register("forValue", transform)
  }

  private def parse(transform:String) : Tree = {
    val fun =
      s"""
         |() => new org.cubefriendly.reflection.ResultTransformer {
         | def transform(result:Iterator[(Vector[String],Vector[String])], args:Map[String, String]):Iterator[(Vector[String],Vector[String])] = {$transform
          |}}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
  def register(name:String, transform:String):Unit = {
    val func = parse(transform)
    _funcs.put(name,newFunc(func))
  }

  registerTop()
  registerSlice()
  registerSearchForValue()

}


class FromDimensionValue extends ResultTransformer {
  override def transform(result: Iterator[(Vector[String], Vector[String])], args: Map[String, String]): Iterator[(Vector[String], Vector[String])] = {
    val term = args("term")
    val dimIndex = args("dimIndex").toInt
    val limit = args("limit").toInt
    val newResult = result.toVector.sortBy({case (vector,metrics) => -metrics.head.toDouble})
    val resultIndex = newResult.indexWhere({case (vector,metrics) => vector(dimIndex) == term})
    val from = Math.max( resultIndex - (limit / 2), 0)
    val to = Math.min(resultIndex + (limit / 2), newResult.length)
    newResult.slice(from, to).toIterator
  }
}

class FromSlice extends ResultTransformer {
  override def transform(result: Iterator[(Vector[String], Vector[String])], args: Map[String, String]): Iterator[(Vector[String], Vector[String])] = {
    val newResult = result.toVector.sortBy({case (vector,metrics) => -metrics.head.toDouble})
    val from = Math.max( args("from").toInt, 0)
    val to = Math.min(args("to").toInt, newResult.length)
    newResult.slice(from, to).toIterator
  }
}

