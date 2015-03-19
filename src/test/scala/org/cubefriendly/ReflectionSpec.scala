package org.cubefriendly

import org.cubefriendly.reflection.Aggregator
import org.specs2.mutable._
/**
 * Cubefriendly
 * Created by david on 12.03.15.
 */
class ReflectionSpec extends Specification {

  "The reflection component" should {
    "be able to build a aggregator for reducing values" in {

      val init = "BigDecimal(0)"
      val finish = "state.toString"
      val reduce = "{state += BigDecimal(value)\nthis}"

      val aggregation:Aggregator = Aggregator.build(init,reduce,finish)
      val result:Aggregator = Seq("1","2","3").foldLeft(aggregation)((sum,elem) => aggregation.reduce(elem))
      result.finish must beEqualTo("6")
    }
  }

}
