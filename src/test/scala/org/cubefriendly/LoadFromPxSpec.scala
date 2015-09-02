package org.cubefriendly

import java.io.File

import org.cubefriendly.data.{QueryBuilder, Cube}
import org.cubefriendly.processors.{Language, DataProcessorProviderImpl, DataProcessorProvider}
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
      val actual: Cube = Await.result(provider.process(name = cubeName, source = pxFile, dest = tmpFile), Duration.Inf)
      val dimensions = actual.dimensions()
      actual.name must be equalTo cubeName
      dimensions must contain(exactly("Vornamen", "Sprachregion", "Geburtsjahr", "Geschlecht"))
      actual.dimension("Vornamen").values.size  must equalTo(29701)

      actual.close()

      val lang = Language("fr")

      val cube = Cube.open(tmpFile)
      val result = QueryBuilder.query(cube).in(lang).where(lang,("Prénoms",Vector("Adrienne"))).run().toVector

      success
    }
  }

}
