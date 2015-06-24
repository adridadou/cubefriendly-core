package org.cubefriendly

import java.io.File

import org.cubefriendly.data.Cube
import org.cubefriendly.processors.{DataProcessorProviderImpl, DataProcessorProvider}
import org.cubefriendly.transformers.DimensionToMetricTransformer
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by davidroon on 23.06.15.
 */
class DimensionToMetricTransformerSpec extends Specification {
  "A Cube data" should {
    def db(): File = File.createTempFile("cube", "friendly")

    "be transformed so certain dimensions become metrics" in {
      val cubeName = "test_cube"
      val processorProvider: DataProcessorProvider = new DataProcessorProviderImpl()
      val transformer = new DimensionToMetricTransformer()

      val csvFile = new File("src/test/resources/test.csv")
      val actual: Cube = Await.result(processorProvider.process(name = cubeName, source = csvFile, dest = db()), Duration.Inf)
      val (metrics, notMetrics) = actual.header().partition(_.contains("Score"))

      transformer.transform(source = actual, metrics = metrics)

      actual.header() must containTheSameElementsAs(notMetrics)
      actual.metrics() must containTheSameElementsAs(metrics)

      success
    }
  }
}