package org.cubefriendly

import java.io.File

import org.cubefriendly.data.Cube
import org.cubefriendly.processors.{DataProcessorProvider, DataProcessorProviderImpl}
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Cubefriendly
 * Created by david on 03.03.15.
 */
class LoadFromCsvSpec extends Specification {
  "A Cube data" should {
    def db(): File = File.createTempFile("cube", "friendly")

    "be loaded from CSV" in {
      val cubeName = "test_cube"
      val provider: DataProcessorProvider = new DataProcessorProviderImpl()
      val csvFile = new File("src/test/resources/banklist.csv")
      val actual: Cube = Await.result(provider.process(name = cubeName, source = csvFile, dest = db()), Duration.Inf)
      val header = actual.header()
      actual.name must be equalTo cubeName
      header must contain(exactly("Bank Name", "City", "ST", "CERT", "Acquiring Institution", "Closing Date", "Updated Date"))
      success
    }
  }

}
