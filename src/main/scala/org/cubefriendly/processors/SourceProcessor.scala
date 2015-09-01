package org.cubefriendly.processors

import java.io.File

import akka.actor.ActorSystem
import akka.stream.io.SynchronousFileSource
import akka.stream.{ActorMaterializer, Materializer}
import org.cubefriendly.CubefriendlyException
import org.cubefriendly.data.Cube
import scaldi.Module

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Cubefriendly
 * Created by david on 11.04.15
 * This code is released under Apache 2 license
 */

trait DataProcessor {

  def name(name:String): DataProcessor

  def process(buffer: Array[Char]):DataProcessor

  def complete(): Cube
}

case class SourceDataHeader(dimensions: Seq[String])

trait DataProcessorProvider {
  implicit val system: ActorSystem
  implicit val materializer: Materializer

  implicit def executor: ExecutionContextExecutor

  def process(name: String, source: File, dest: File): Future[Cube]
  def getProcessorByFilename(name:String, destination:File):DataProcessor
}

class DataProcessorModule extends Module {
  bind[DataProcessorProvider] to injected[DataProcessorProviderImpl]
}

class DataProcessorProviderImpl extends DataProcessorProvider {
  override implicit val system: ActorSystem = ActorSystem()
  override implicit val executor: ExecutionContextExecutor = system.dispatcher
  override implicit val materializer: Materializer = ActorMaterializer()

  override def process(name: String, source: File, dest: File): Future[Cube] = {
    val processor = getProcessorByFilename(source.getName,dest)
    SynchronousFileSource(source).runFold(processor)(
      (processor, a) => processor.process(a.decodeString("UTF-8").toCharArray)
    ).map(_.complete())
  }

  def getProcessorByFilename(n:String, dest:File):DataProcessor = n match {
    case name if name.endsWith(".px") => new PxProcessor(dest).name(name)
    case name if name.endsWith(".csv") => new CsvProcessor(dest).name(name)
    case name => throw new CubefriendlyException("could not determine the file type of " + name)
  }

}

case class CubeConfig(name: String, metrics: Seq[String])
