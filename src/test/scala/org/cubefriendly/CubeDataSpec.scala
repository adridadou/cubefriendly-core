package org.cubefriendly

import java.io.File

import org.cubefriendly.data.{Cube, QueryBuilder}
import org.cubefriendly.reflection.Aggregator
import org.specs2.mutable._

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 * This code is released under Apache 2 license
 */
class CubeDataSpec extends Specification  {

  def db(): File = File.createTempFile("cube", "friendly")

  "A Cube data builder" should {

    "get a name to identify the cube we're creating" in {
      val cubeName = "test_cube"
      val actual:Cube = Cube.builder(db()).name(cubeName).toCube

      actual.name must be equalTo cubeName
    }

    "set a header " in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).dimensions(header).name(cubeName).toCube

      actual.dimensions() must contain(exactly("year","country","debt"))
    }

    "add a record" in {
      val cubeName = "test_cube"
      val dimensions = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).dimensions(dimensions).record(Vector("1990","Switzerland","30000000")).name(cubeName).toCube

      actual.dimensions() must contain(exactly("year","country","debt"))

      actual.dimension("year").values must contain(exactly("1990"))
      actual.dimension("country").values must contain(exactly("Switzerland"))
      actual.dimension("debt").values must contain(exactly("30000000"))
    }

    "add a values only if it does not exist already" in {
      val cubeName = "test_cube"
      val dimensions = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).dimensions(dimensions)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .name(cubeName)
        .toCube

      actual.dimensions() must contain(exactly("year","country","debt"))

      actual.dimension("year").values must contain(exactly("1990", "1995"))
      actual.dimension("country").values must contain(exactly("Switzerland", "France"))
      actual.dimension("debt").values must contain(exactly("30000000", "3000000"))
    }

    "query the values" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).dimensions(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .name(cubeName)
        .toCube

      val actual = QueryBuilder.query(cube).where(Map(
        "country" -> Vector("Switzerland")
      )).run().map({ case (key, value) => key })

      actual must contain(exactly(Vector("1990","Switzerland","30000000"), Vector("1990","Switzerland","3000000")))
    }

    "query the values with no map" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).dimensions(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .name(cubeName)
        .toCube

      val actual = QueryBuilder.query(cube).where(
        "country" -> Vector("Switzerland"),
        "year" -> Vector("1990")
      ).run().map({ case (key, value) => key })

      actual must contain(exactly(Vector("1990","Switzerland","30000000"), Vector("1990","Switzerland","3000000")))
    }
  }
}
