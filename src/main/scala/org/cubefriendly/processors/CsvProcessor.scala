package org.cubefriendly.processors

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.data.Cube

import scala.io.Source

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 */
class CsvProcessor(val lines: Iterator[String], val csvHeader: CsvHeader) extends DataProcessor {
  lazy val header: SourceDataHeader = csvHeader.dataHeader
  override def process(config: CubeConfig, db: File): Cube = {
    val builder = Cube.builder(db).header(lines.next().split(csvHeader.separator).toVector).metrics(config.metrics: _*)
    lines.filter(_.nonEmpty).foreach(line => builder.record(line.split(csvHeader.separator).toVector))
    builder.name(config.name).toCube
  }
}

object CsvProcessor {

  val separators = Seq(",",";","\t")

  def apply(file:File):CsvProcessor = {
    val iterator = Source.fromFile(file).getLines()
    val lines = Source.fromFile(file).getLines()
    lines.drop(1) // we don't need the header
    new CsvProcessor(lines,header(iterator))
  }

  private def header(lines:Iterator[String]):CsvHeader = {
    val csvHeader = lines.next()
    val entry = lines.next()
    val headerResult = separators.map(c => c -> csvHeader.split(c).toSeq).toMap
    val entryResult = separators.map(c => c -> entry.split(c).toSeq).toMap

    (for(s <- separators if headerResult(s).length > 1 && headerResult(s).length == entryResult(s).length) yield s) match {
      case result if result.isEmpty => throw new CubefriendlyException("no suitable separator found")
      case result if result.length > 1 => throw new CubefriendlyException("found more than one possible separator. Please select the correct one: " + result)
      case r => CsvHeader(r.head,headerResult(r.head))
    }
  }
}

case class CsvHeader(separator: String, dimensions: Seq[String]) {
  lazy val dataHeader: SourceDataHeader = SourceDataHeader(dimensions)
}
