package org.cubefriendly.processors

import java.io.File

import akka.actor.ActorSystem
import akka.stream.io.SynchronousFileSource
import akka.stream.{ActorFlowMaterializer, FlowMaterializer}
import org.cubefriendly.data.Cube
import scaldi.Module

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 */

trait DataProcessor {
  def process(buffer: Array[Char]): DataProcessor

  def complete(): Cube
}

case class SourceDataHeader(dimensions: Seq[String])

trait DataProcessorProvider {
  implicit val system: ActorSystem
  implicit val materializer: FlowMaterializer

  implicit def executor: ExecutionContextExecutor

  def process(name: String, source: File, dest: File): Future[Cube]
}

class DataProcessorModule extends Module {
  bind[DataProcessorProvider] to injected[DataProcessorProviderImpl]
}

class DataProcessorProviderImpl extends DataProcessorProvider {
  override implicit val system: ActorSystem = ActorSystem()
  override implicit val executor: ExecutionContextExecutor = system.dispatcher
  override implicit val materializer: FlowMaterializer = ActorFlowMaterializer()

  override def process(name: String, source: File, dest: File): Future[Cube] = {
    val processor = new CsvProcessor(dest).name(name)
    SynchronousFileSource(source).runFold(processor)(
      (processor, a) => processor.process(a.decodeString("UTF-8").toCharArray)
    ).map(_.complete())
  }

}

case class CubeConfig(name: String, metrics: Seq[String])