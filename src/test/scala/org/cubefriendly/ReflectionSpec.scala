package org.cubefriendly

import org.cubefriendly.reflection.{DimensionValuesSelector, Aggregator}
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

    "be able to search for a term in a list" in {
      val search = DimensionValuesSelector.funcs("search")

      search.select("abc","e") should equalTo(false)
      search.select("abc","b") should equalTo(true)
    }

  }

}
