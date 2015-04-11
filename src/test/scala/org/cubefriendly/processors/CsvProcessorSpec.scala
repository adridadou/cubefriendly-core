package org.cubefriendly.processors

import java.io.File

import org.specs2.mutable.Specification

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 */
class CsvProcessorSpec extends Specification {
  "A CSV processor" should {
    "read the header" in {
      val csvFile = new File("src/test/resources/banklist.csv")
      val processor:CsvProcessor = CsvProcessor(file = csvFile)
      val header = processor.header
      header.separator must be(",")
      header.dimensions must contain(exactly("Bank Name","City","ST","CERT","Acquiring Institution","Closing Date","Updated Date"))
    }
  }
}
