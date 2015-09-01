package org.cubefriendly

import java.io.File

import org.cubefriendly.data.Cube
import org.cubefriendly.processors.{DataProcessorProviderImpl, DataProcessorProvider}
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
    def db(): File = File.createTempFile("cube", "friendly")

    "be loaded from CSV" in {
      val cubeName = "px-x-Vornamen_F.px"
      val provider: DataProcessorProvider = new DataProcessorProviderImpl()
      val pxFile = new File("src/test/resources/px-x-Vornamen_F.px")
      val actual: Cube = Await.result(provider.process(name = cubeName, source = pxFile, dest = db()), Duration.Inf)
      val dimensions = actual.dimensions()
      actual.name must be equalTo cubeName
      dimensions must contain(exactly("Vornamen", "Sprachregion", "Geburtsjahr", "Geschlecht"))
      actual.dimension("Vornamen").values.size  must equalTo(29701)
      success
    }
  }

}
