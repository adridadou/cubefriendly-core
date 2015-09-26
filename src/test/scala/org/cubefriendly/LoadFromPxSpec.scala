package org.cubefriendly

import java.io.File

import org.cubefriendly.data.{QueryBuilder, Cube}
import org.cubefriendly.processors.{Language, DataProcessorProviderImpl, DataProcessorProvider}
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.scalameter._

/**
 * Cubefriendly
 * Created by davidroon on 01.09.15.
 * This code is released under Apache 2 license
 */
class LoadFromPxSpec extends Specification {
  "A Cube data" should {
    val tmpFile = File.createTempFile("vornamen","px")

    "be loaded from PX file" in {
      val cubeName = "px-x-Vornamen_F.px"
      val provider: DataProcessorProvider = new DataProcessorProviderImpl()
      val pxFile = new File("src/test/resources/px-x-Vornamen_F.px")
      val time = measure {
        val actual: Cube = Await.result(provider.process(name = cubeName, source = pxFile, dest = tmpFile), Duration.Inf)
        val dimensions = actual.dimensions()
        val numberOfFirstnames = 29701
        actual.name must be equalTo cubeName
        dimensions must contain(exactly("Vornamen", "Sprachregion", "Geburtsjahr", "Geschlecht"))
        actual.dimension("Vornamen").size must equalTo(numberOfFirstnames)
        actual.close()
      }

      println(s"Total creation time: $time")
      val cube = Cube.open(tmpFile)

      val lang = Language("fr")

      var result:Iterator[(Vector[String], Vector[String])] = Iterator()

      val years = (1900 until 2004).map(_.toString).toVector

      val time2 = measure {
        result = QueryBuilder.query(cube).in(lang)
          .eliminate(lang, "Année de naissance")
          .where(lang,"Année de naissance" -> years)
          .where(lang, "Région linguistique" -> Vector("Suisse"))
          .run()
      }

      println(s"Total query time: $time2")

      val time3 = measure {
        val test = result.toVector
        test
      }

      println(s"Total vector time: $time3")

      val time4 = measure {
        result = QueryBuilder.query(cube).in(lang)
          .eliminate(lang, "Année de naissance")
          .where(lang, "Année de naissance" -> years)
          .where(lang, "Région linguistique" -> Vector("Suisse"))
          .run()
      }

      println(s"Total query time: $time4")
      success
    }
  }

}
