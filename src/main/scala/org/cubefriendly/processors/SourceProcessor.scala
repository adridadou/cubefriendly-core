package org.cubefriendly.processors

import java.io.File

import org.cubefriendly.data.Cube
import org.mapdb.DB
import scaldi.Module

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 */
trait DataProcessor {
  def header():DataHeader

  def process(config: CubeConfig, db: DB): Cube
}

trait DataHeader {
  def dimensions:Seq[String]
}

trait DataProcessorProvider {
  def forSource(file: File): DataProcessor
}

class DataProcessorModule extends Module {
  bind[DataProcessorProvider] to injected[DataProcessorProviderImpl]
}

class DataProcessorProviderImpl extends DataProcessorProvider {
  override def forSource(file: File): DataProcessor = CsvProcessor(file = file)
}

case class CubeConfig(name: String, metrics: Seq[String])