package org.cubefriendly

import java.io.File

import org.cubefriendly.data.{QueryBuilder, Cube}
import org.cubefriendly.reflection.Aggregator
import org.mapdb.{DB, DBMaker}
import org.specs2.mutable._

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */
class CubeDataSpec extends Specification  {

  def db():DB = DBMaker.newTempFileDB().make()

  "A Cube data builder" should {

    "get a name to identify the cube we're creating" in {
      val cubeName = "test_cube"
      val actual:Cube = Cube.builder(db()).toCube(cubeName)

      actual.name must be equalTo cubeName
    }

    "set a header " in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).header(header).toCube(cubeName)

      actual.header must contain(exactly("year","country","debt"))
    }

    "add a record" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).header(header).record(Vector("1990","Switzerland","30000000")).toCube(cubeName)

      actual.header must contain(exactly("year","country","debt"))

      actual.dimensions("year").values must contain(exactly("1990"))
      actual.dimensions("country").values must contain(exactly("Switzerland"))
      actual.dimensions("debt").values must contain(exactly("30000000"))
    }

    "add a values only if it does not exist already" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val actual:Cube = Cube.builder(db()).header(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .toCube(cubeName)

      actual.header must contain(exactly("year","country","debt"))

      actual.dimensions("year").values must contain(exactly("1990","1995"))
      actual.dimensions("country").values must contain(exactly("Switzerland","France"))
      actual.dimensions("debt").values must contain(exactly("30000000","3000000"))
    }

    "query the values" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).header(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .toCube(cubeName)

      val actual = QueryBuilder.query(cube).where(Map(
        "country" -> Vector("Switzerland")
      )).run().map(_._1)

      actual must contain(exactly(Vector("1990","Switzerland","30000000"), Vector("1990","Switzerland","3000000")))
    }

    "query the values with no map" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).header(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .toCube(cubeName)

      val actual = QueryBuilder.query(cube).where(
        "country" -> Vector("Switzerland"),
        "year" -> Vector("1990")
      ).run().map(_._1)

      actual must contain(exactly(Vector("1990","Switzerland","30000000"), Vector("1990","Switzerland","3000000")))
    }

    "aggregate values with sum function" in {
      Aggregator.registerSum()
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).header(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1995","France","30000000"))
        .record(Vector("1995","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .toCube(cubeName)

      val actual = QueryBuilder.query(cube).where(
        "country" -> Vector("Switzerland"),
        "year" -> Vector("1990")
      ).groupBy("year","country").reduce("debt" -> "sum").run().map(_._1)

      actual must contain(exactly(Vector("1990","Switzerland","33000000")))
    }

    "aggregate values with sum function and eliminate one" in {
      Aggregator.registerSum()
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val cube:Cube = Cube.builder(db()).header(header)
        .record(Vector("1990","Switzerland","30000000"))
        .record(Vector("1990","France","30000000"))
        .record(Vector("1990","France","3000000"))
        .record(Vector("1990","Switzerland","3000000"))
        .toCube(cubeName)

      val actual = QueryBuilder.query(cube).where(
        "year" -> Vector("1990")
      ).groupBy("year").reduce("debt" -> "sum").run().map(_._1)

      actual must contain(exactly(Vector("1990","66000000")))
    }

    "open a cube that was previously created" in {
      val cubeName = "test_cube"
      val header = Vector("year","country","debt")
      val file = File.createTempFile("test","cube")
      val actual:Cube = Cube.builder(DBMaker.newFileDB(file).make()).header(header).record(Vector("1990","Switzerland","30000000")).toCube(cubeName)

      actual.header must contain(exactly("year","country","debt"))

      actual.dimensions("year").values must contain(exactly("1990"))
      actual.dimensions("country").values must contain(exactly("Switzerland"))
      actual.dimensions("debt").values must contain(exactly("30000000"))

      actual.close()

      val reopen:Cube = Cube.open(file)

      reopen.header must contain(exactly("year","country","debt"))

      reopen.dimensions("year").values must contain(exactly("1990"))
      reopen.dimensions("country").values must contain(exactly("Switzerland"))
      reopen.dimensions("debt").values must contain(exactly("30000000"))
    }

  }
}
