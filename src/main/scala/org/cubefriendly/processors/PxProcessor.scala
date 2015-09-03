package org.cubefriendly.processors

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.data.{CubeBuilder, Cube}

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Cubefriendly
 * Created by davidroon on 17.08.15.
 * This code is released under Apache 2 license
 */


case class Key(name:String, dimension:Option[String] = None, lang:Option[Language])

abstract sealed class PxStreamState
case class HeaderReader(key:mutable.StringBuilder = StringBuilder.newBuilder, dimension:Option[String] = None, lang:Option[Language] = None) extends PxStreamState
case class HeaderReaderLang(s:HeaderReader, lang:mutable.StringBuilder = mutable.StringBuilder.newBuilder) extends PxStreamState
case class HeaderReaderDimension(s:HeaderReader, dimension:mutable.StringBuilder = mutable.StringBuilder.newBuilder) extends PxStreamState
case class HeaderReaderValue(key:Key, values:mutable.Buffer[String]) extends PxStreamState
case class HeaderReaderStringValue(key:Key, values:mutable.Buffer[String], builder:mutable.StringBuilder) extends PxStreamState
case class HeaderReaderNumberValue(key:Key, values:mutable.Buffer[String], builder:mutable.StringBuilder) extends PxStreamState

case class DataReader(cube:CubeBuilder, dimensions:Vector[String], vector:VectorIncrementer) extends PxStreamState
case class DataNumberReader(builder:mutable.StringBuilder, vector:VectorIncrementer, cube:CubeBuilder) extends PxStreamState
case class DataStringReader(builder:mutable.StringBuilder, vector:VectorIncrementer, cube:CubeBuilder) extends PxStreamState

case class EndOfFile(cube:Cube)  extends PxStreamState

case class CubeHeader(dimensions:Vector[Dimension], languages:Vector[Language], language: Option[Language])
case class Dimension(name:String, values:Seq[DimensionValue], lang:Option[Language])
case class DimensionValue(code:String, value:String)
case class Language(code:String)

object PxProcessor {
  def apply(file:File) :PxProcessor = new PxProcessor(file)
}

class PxProcessor(file:File) extends DataProcessor {

  var name:String = ""
  var state: PxStreamState = HeaderReader()
  var dimensions:Map[String,Dimension] = Map()
  var langDimensions:Map[String, Map[Language,Dimension]] = Map()
  val header:mutable.Buffer[(Key,Vector[String])] = mutable.Buffer[(Key,Vector[String])]()

  def defaultEncoding:String = "windows-1252"

  override def process(buffer: Array[Char]): DataProcessor = {
    buffer foreach read
    this
  }

  def name(name: String): DataProcessor = {
    this.name = name
    this
  }

  private def read(c:Char) : PxStreamState = {
    c match {
      case '\n' =>
      case '\r' =>
      case _ => routeState(c,state)
    }
    state
  }

  private def routeState(c:Char, s:PxStreamState) : Unit = state = s match {
      case s:HeaderReader => readHeaderKey(c,s)
      case s:HeaderReaderLang => readLang(c,s)
      case s:HeaderReaderDimension => readDimension(c,s)
      case s:HeaderReaderValue => startReaderHeaderValue(c, s)
      case s:HeaderReaderStringValue => readHeaderValue(c, s)
      case s:HeaderReaderNumberValue => readHeaderValue(c, s)
      case s:DataReader => startReadDataValue(c, s)
      case s:DataNumberReader => readData(c,s)
      case s:DataStringReader => readData(c,s)
      case s:EndOfFile => s
  }

  private def readDimension(c:Char, s:HeaderReaderDimension) : PxStreamState = {
    c match {
      case ')' => HeaderReader(key = s.s.key, dimension = Some(s.dimension.deleteCharAt(0).deleteCharAt(s.dimension.length - 1).toString()), lang = s.s.lang)
      case _ =>
        s.dimension.append(c)
        s
    }
  }

  private def readLang(c:Char, s:HeaderReaderLang) : PxStreamState = {
    c match {
      case ']' =>
        HeaderReader(key = s.s.key, dimension = s.s.dimension, lang = Some(Language(s.lang.toString())))
      case _ =>
        s.lang.append(c)
        s
    }
  }

  private def readHeaderKey(c:Char, s:HeaderReader):PxStreamState = {
    c match {
      case '=' if s.key.toString() == "DATA" => prepareCubeForDataReading()
      case '=' =>
        HeaderReaderValue(Key(name = s.key.toString(), dimension = s.dimension, lang = s.lang), mutable.Buffer[String]())
      case '(' => HeaderReaderDimension(s)
      case '[' => HeaderReaderLang(s)
      case _ =>
        s.key.append(c)
        s
    }
  }

  private def startReaderHeaderValue(c:Char, s:HeaderReaderValue) : PxStreamState = {
    c match {
      case '\"' => HeaderReaderStringValue(s.key,s.values, mutable.StringBuilder.newBuilder)
      case ',' => s
      case ';' =>
        header += ((s.key,s.values.toVector))
        HeaderReader()
      case _ => HeaderReaderNumberValue(s.key,s.values,mutable.StringBuilder.newBuilder.append(c))
    }
  }

  private def readHeaderValue(c:Char, s:HeaderReaderStringValue) : PxStreamState = {
    c match {
      case '\"' if s.builder.charAt(s.builder.length - 1) != '\\' =>
        s.values += s.builder.toString
        s.builder.clear()
        HeaderReaderValue(key = s.key, values = s.values)
      case _ =>
        s.builder.append(c)
        s
    }
  }

  private def readHeaderValue(c:Char, s:HeaderReaderNumberValue) : PxStreamState = {
    c match {
      case ';' =>
        if(s.builder.toString() != "0") {
          s.values += s.builder.toString()
          header += ((s.key,s.values.toVector))
        }
        HeaderReader()
      case ',' =>
        s.values += s.builder.toString
        s.builder.clear()
        s
      case _ =>
        s.builder.append(c)
        s
    }
  }

  private def startReadDataValue(c:Char, s:DataReader): PxStreamState = {
    c match {
      case '\"' => DataStringReader(mutable.StringBuilder.newBuilder,s.vector,s.cube)
      case _ => DataNumberReader(mutable.StringBuilder.newBuilder,s.vector, s.cube)
    }
  }

  private def prepareCubeForDataReading():PxStreamState = {
    val cubeBuilder = Cube.builder(file).name(name)
    header.find({case (key,values) => key.name == "LANGUAGE"}) match {
      case Some((key,values)) =>
        values.headOption.map(Language.apply) match {
          case Some(lang) => createCubeHeaderWithLangs(lang, cubeBuilder)
          case None => throw new CubefriendlyException("LANGUAGE property defined but without a value!")
        }
      //case None => createCubeHeadWithoutLang(cubeBuilder) TODO: handle this case too
    }
  }

  private def createCubeHeaderWithLangs(defaultLang:Language, cubeBuilder: CubeBuilder) : PxStreamState = {
    val pxHeader:Map[String, Map[Language, mutable.Buffer[(Key,Vector[String])]]] = header.groupBy({case (key,values) => key.name}).mapValues(_.groupBy({case (key,values) => key.lang.getOrElse(defaultLang)}))
    val langs = pxHeader("LANGUAGES")(defaultLang).map({case (_,values) => values.map(Language.apply)}).head
    val codes: Vector[Vector[String]] = prepareCodes(cubeBuilder, defaultLang, pxHeader)

    prepareDimensions(defaultLang, cubeBuilder, pxHeader, langs, codes)
  }

  def prepareCodes(cubeBuilder: CubeBuilder, defaultLang:Language, pxHeader:Map[String, Map[Language, mutable.Buffer[(Key,Vector[String])]]]): Vector[Vector[String]] = {
    val codesMap = pxHeader("CODES")(defaultLang).map({case (key,values) => key.dimension match {
      case Some(dimension) => dimension -> values
      case None => throw new CubefriendlyException("CODES property has been defined without specifying to which dimension it belongs to!")
    }}).toMap
    val dimensions = getDimensions(defaultLang,pxHeader)
    cubeBuilder.dimensions(dimensions)

    val codes = dimensions.map(dimension => codesMap.get(dimension) match {
      case Some(values) => values
      case None => pxHeader("VALUES")(defaultLang).find({ case (key, _) => key.dimension.get == dimension }).map({ case (_, values) => values }).get.indices.map(_.toString).toVector
    })

    cubeBuilder.prepareCodes(codes)
    codes
  }

  def prepareDimensions(defaultLang: Language, cubeBuilder: CubeBuilder, pxHeader: Map[String, Map[Language, mutable.Buffer[(Key, Vector[String])]]], langs: Vector[Language], codes: Vector[Vector[String]]): DataReader = {
    langs.foreach(lang => {
      val dimensionsInLang = getDimensions(lang, pxHeader)
      val data = dimensionsInLang.map(dimension => {
        pxHeader("VALUES")(lang).find({ case (key, _) => key.dimension.getOrElse("") == dimension }).map({ case (_, v) => v }) match {
          case Some(values) => values
          case None => throw new CubefriendlyException("no values found for dimension " + dimension + " in " + lang.code)
        }
      })
      cubeBuilder.dimensions(lang, data)
      cubeBuilder.dimensions(dimensionsInLang, lang)
    })
    cubeBuilder.metrics("VALUE")

    DataReader(cubeBuilder, getDimensions(defaultLang, pxHeader), new VectorIncrementer(codes))
  }

  private def getDimensions(lang:Language, pxHeader:Map[String, Map[Language, mutable.Buffer[(Key,Vector[String])]]]) : Vector[String] = {
    val stubs:Vector[String] = pxHeader("STUB")(lang)
      .map({case (_,values) => values}).headOption match {
      case Some(s) => s
      case None => throw new CubefriendlyException("no STUB property found for language " + lang.code)
    }
    val heading:Vector[String] = pxHeader("HEADING")(lang)
      .map({case (_,values) => values}).headOption match {
      case Some(h) => h
      case None => throw new CubefriendlyException("no HEADING property found for language " + lang.code)
    }
    stubs ++ heading
  }

  private def addRecord(s:DataNumberReader):PxStreamState = {
    s.vector.inc()
    if(s.builder.toString() != "0") {
      val vector = s.vector.currentVector
      s.cube.record(vector, s.builder.toString())
    }
    s.builder.clear()
    s
  }

  private def addRecord(s:DataStringReader):PxStreamState = {
    s.vector.inc()
    val vector = s.vector.currentVector
    s.cube.record(vector, s.builder.toString())
    s.builder.clear()
    s
  }

  private def readData(c:Char, s:DataNumberReader):PxStreamState = {
    c match {
      case ';' =>
        //EOF
        addRecord(s)
        EndOfFile(s.cube.toCube)
      case '\n' if s.builder.nonEmpty => addRecord(s)
      case ' ' if s.builder.nonEmpty => addRecord(s)
      case '\n' => s
      case '\r' => s
      case ' ' => s
      case _ =>
        s.builder.append(c)
        s
    }
  }

  private def readData(c:Char, s:DataStringReader):PxStreamState = {
    c match {
      case ';' =>
        //EOF
        EndOfFile(s.cube.toCube)
      case '\"' if s.builder.nonEmpty && s.builder.charAt(s.builder.length - 1) != '\\' => addRecord(s)
      case '\n' if s.builder.isEmpty => s
      case '\r' if s.builder.isEmpty => s
      case ' ' if s.builder.isEmpty => s
      case '\"' if s.builder.isEmpty => s
      case _ =>
        s.builder.append(c)
        s
    }
  }

  override def complete(): Cube = {
    state match {
      case s:EndOfFile => s.cube
      case s => throw new CubefriendlyException("no data has been found in this cube!")
    }
  }
}

class VectorIncrementer(values:Vector[Vector[String]]) extends Iterator[Vector[Int]]{
  var end:Boolean = false
  val vector:Array[Int] = new Array[Int](values.length).map(i => 1)
  def inc():Unit = inc(vector.length - 1)

  @tailrec
  private def inc(pos: Int):Unit = {
    if (end) {
      throw new RuntimeException("max value exceeded!")
    }
    if (pos < 0) {
      end = true
    } else {
      vector(pos) += 1
      if (vector(pos) > values(pos).length) {
        vector(pos) = 1
        inc(pos - 1)
      }
    }
  }

  def currentVector: Vector[Int] = vector.toVector

  def hasNext: Boolean = !end

  def next(): Vector[Int] = {
    inc()
    currentVector
  }
}
