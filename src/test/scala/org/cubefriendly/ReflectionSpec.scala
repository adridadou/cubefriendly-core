package org.cubefriendly

import org.cubefriendly.reflection.{ResultTransformer, Aggregator}
import org.specs2.mutable._
/**
 * Cubefriendly
 * Created by david on 12.03.15.
 */
class ReflectionSpec extends Specification {

  "The reflection component" should {
    "be able to build a aggregator for reducing values" in {

      val init = "0D"
      val finish = "state"
      val reduce = "{state += value.toDouble\nthis}"

      val aggregation:Aggregator = Aggregator.build(Aggregator.parse(init,reduce,finish))
      val result:Aggregator = Seq("1","2","3").foldLeft(aggregation)((sum,elem) => aggregation.reduce(elem))
      result.finish must beEqualTo(6d)
    }

    "be able to transform result with the top x" in {
      val result = Iterator((Vector("1"),Vector("1")),(Vector("2"),Vector("2")),(Vector("3"),Vector("3")),(Vector("a"),Vector("a")))
      val top3 = ResultTransformer.funcs("top").transform(result,Map("from" -> "0", "to" -> "2"))

      println(top3.toVector)

      success
    }

  }

}
