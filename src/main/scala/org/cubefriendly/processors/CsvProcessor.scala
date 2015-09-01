package org.cubefriendly.processors

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.data.{Cube, CubeBuilder}

import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 11.04.15.
 * This code is released under Apache 2 license
 */


abstract sealed class CsvStreamState {
  def builder: mutable.StringBuilder
}

object CsvProcessor {
  def apply(file: File): CsvProcessor = new CsvProcessor(file)
}

case class CsvReadFirstRow(builder: mutable.StringBuilder) extends CsvStreamState

case class CsvReadSecondRow(first: String, builder: mutable.StringBuilder) extends CsvStreamState

case class CsvReadRest(header: CsvHeader, cubeBuilder: CubeBuilder, builder: mutable.StringBuilder) extends CsvStreamState

class CsvProcessor(db: File) extends DataProcessor {
  private val separators = Seq(",", ";", "\t","|")

  private var state: CsvStreamState = CsvReadFirstRow(mutable.StringBuilder.newBuilder)
  private var cubeName: String = ""

  override def process(buffer: Array[Char]): CsvProcessor = {
    buffer foreach read
    this
  }

  private def read(c: Char): Unit = c match {
    case '\n' =>
      state = state match {
        case s: CsvReadFirstRow => fsm(s)
        case s: CsvReadSecondRow => fsm(s)
        case s: CsvReadRest => fsm(s)
      }
    case char: Char => state.builder.append(char)
  }

  private def fsm(s: CsvReadFirstRow): CsvStreamState = CsvReadSecondRow(s.builder.toString(), StringBuilder.newBuilder)

  private def fsm(s: CsvReadSecondRow): CsvStreamState = {
    val cubeBuilder = Cube.builder(db)
    val header = buildHeader(s.first, s.builder.toString())
    cubeBuilder.dimensions(header.dimensions)
    CsvReadRest(header, cubeBuilder, StringBuilder.newBuilder)
  }

  private def buildHeader(first: String, second: String): CsvHeader = {
    val csvHeader = first
    val entry = second
    val headerResult = separators.map(c => c -> clean(csvHeader.split(c).toVector)).toMap
    val entryResult = separators.map(c => c -> clean(entry.split(c).toVector)).toMap

    (for(s <- separators if headerResult(s).length > 1 && headerResult(s).length == entryResult(s).length) yield s) match {
      case result if result.isEmpty => throw new CubefriendlyException("no suitable separator found")
      case result if result.length > 1 => throw new CubefriendlyException("found more than one possible separator. Please select the correct one: " + result)
      case r => CsvHeader(r.head,headerResult(r.head))
    }
  }

  private def clean(s: Vector[String]): Vector[String] = s.map(_.trim())

  private def fsm(s: CsvReadRest): CsvStreamState = {
    val line = s.builder.toString()
    if (line.nonEmpty) {
      s.cubeBuilder.record(clean(line.split(s.header.separator).toVector))
    }
    CsvReadRest(s.header, s.cubeBuilder, mutable.StringBuilder.newBuilder)
  }

  def name(name: String): DataProcessor = {
    cubeName = name
    this
  }

  override def complete(): Cube = state match {
    case s: CsvReadFirstRow => throw new CubefriendlyException("a CSV file cannot have only one line. We need also content!")
    case s: CsvReadSecondRow => throw new CubefriendlyException("The case where a CSV has only two lines is not handled yet. Please report a bug if you need it!")
    case s: CsvReadRest => s.cubeBuilder.name(cubeName).toCube
  }
}

case class CsvHeader(separator: String, dimensions: Vector[String]) {
  lazy val dataHeader: SourceDataHeader = SourceDataHeader(dimensions)
}
