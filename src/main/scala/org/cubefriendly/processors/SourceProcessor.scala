package org.cubefriendly.processors

import java.io.File

import scaldi.{Injectable, Injector, Module}

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 */
trait DataProcessor {
  def header():DataHeader
}

trait DataHeader {
  def dimensions:Seq[String]
}

trait DataProcessorProvider {
  def forSource(file: File): DataProcessor
}

class DataProcessorModule extends Module {
  bind[DataProcessorProvider] to new DataProcessorProviderImpl
}

class DataProcessorProviderImpl(implicit inj: Injector) extends DataProcessorProvider with Injectable {
  override def forSource(file: File): DataProcessor = CsvProcessor(file = file)
}