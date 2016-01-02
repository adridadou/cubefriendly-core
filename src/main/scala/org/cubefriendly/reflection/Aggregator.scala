package org.cubefriendly.reflection

import scala.reflect.runtime.universe._

/**
 * Cubefriendly
 * Created by davidroon on 04.10.15.
 * This code is released under Apache 2 license
 */
trait Aggregator {
  def reduce(value:String):Aggregator
  def finish:Double
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
