package org.cubefriendly.processors

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