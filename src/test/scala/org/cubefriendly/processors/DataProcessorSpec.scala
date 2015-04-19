package org.cubefriendly.processors

import java.io.File

import org.specs2.mutable.Specification
import scaldi.Injectable


/**
 * Cubefriendly
 * Created by david on 19.04.15.
 */
class DataProcessorSpec extends Specification with Injectable {
  implicit val module = new DataProcessorModule

  val provider = inject[DataProcessorProvider]
  "DataProcessorProvider" should {
    "return the CsvProcessor when the file is a CSV" in {
      val csvFile = new File("src/test/resources/banklist.csv")
      provider.forSource(csvFile) match {
        case processor: CsvProcessor => success
        case processor => failure("the processor should be a CsvProcessor")
      }
    }
  }
}
