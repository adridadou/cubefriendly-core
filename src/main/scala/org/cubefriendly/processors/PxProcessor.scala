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
      case _ =>
        state = state match {
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
    }
    state
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
      case '\"' if s.builder.last != '\\' =>
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
        s.values += s.builder.toString
        header += ((s.key,s.values.toVector))
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
      case Some((key,values)) => createCubeHeaderWithLangs(Language(values.head), cubeBuilder)
     // case None => createCubeHheadWithoutLang(cubeBuilder)
    }
  }

  private def createCubeHeaderWithLangs(defaultLang:Language, cubeBuilder: CubeBuilder) : PxStreamState = {
    val pxHeader:Map[String, Map[Language, mutable.Buffer[(Key,Vector[String])]]] = header.groupBy({case (key,values) => key.name}).mapValues(_.groupBy({case (key,values) => key.lang.getOrElse(defaultLang)}))
    val langs = pxHeader("LANGUAGES")(defaultLang).map({case (_,values) => values.map(Language.apply)}).head

    langs.foreach(lang => {
      val data = getDimensions(lang,pxHeader).map(dimension =>{
        val values = pxHeader("VALUES")(lang).find({case (key,_) => key.dimension.getOrElse("") == dimension}).map({case (_,v) => v}).get
        dimension -> (values ++ Vector("VALUE"))
      }).toMap
      cubeBuilder.dimensions(lang,data)
    })

    val codesMap = pxHeader("CODES")(defaultLang)
      .map({case (key,values) => key.dimension.get -> values}).toMap
    val dimensions = getDimensions(defaultLang,pxHeader)
    cubeBuilder.dimensions(dimensions)
    cubeBuilder.metrics("VALUES")
    val codes = dimensions.map(dimension => codesMap.get(dimension) match {
      case Some(values) => values
      case None => pxHeader("VALUES")(defaultLang).find({case (key,_) => key.dimension.get == dimension}).map({case (_,values) => values}).get.indices.map(_.toString).toVector
    }) ++ Vector(Vector("0"))
    DataReader(cubeBuilder, getDimensions(defaultLang,pxHeader), new VectorIncrementer(codes))
  }

  private def getDimensions(lang:Language, pxHeader:Map[String, Map[Language, mutable.Buffer[(Key,Vector[String])]]]) : Vector[String] = {
    val stubs:Vector[String] = pxHeader("STUB")(lang)
      .map({case (_,values) => values}).head
    val heading:Vector[String] = pxHeader("HEADING")(lang)
      .map({case (_,values) => values}).head
    stubs ++ heading
  }

  private def readData(c:Char, s:DataNumberReader):PxStreamState = {
    c match {
      case ';' =>
        //EOF
        val vector = s.vector.next().toVector ++ Vector(s.builder.toDouble.toString)
        s.cube.record(vector)
        s.builder.clear()
        EndOfFile(s.cube.toCube)
      case '\n' if s.builder.nonEmpty =>
        val vector = s.vector.next().toVector ++ Vector(s.builder.toDouble.toString)
        s.cube.record(vector)
        s.builder.clear()
        s
      case ' ' if s.builder.nonEmpty =>
        val value = s.builder.toDouble.toString
        val array = s.vector.next()
        array(array.length - 1) = value
        s.cube.record(array.toVector)
        s.builder.clear()
        s
      case _ =>
        s.builder.append(c)
        s
    }
  }

  private def readData(c:Char, s:DataStringReader):PxStreamState = {
    c match {
      case ';' =>
        //EOF
        s
      case ',' if s.builder.head != '\"' =>
        s.builder.clear()
        s
      case ',' =>
        s.builder.clear()
        s
      case '\"' if s.builder.isEmpty || s.builder.charAt(s.builder.length - 1) != '\\' =>
        s
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

class VectorIncrementer(values:Vector[Vector[String]]) extends Iterator[Array[String]]{
  var end:Boolean = false
  val vector:Array[Int] = new Array(values.length)
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
      if (vector(pos) == values(pos).length) {
        vector(pos) = 0
        inc(pos - 1)
      }
    }
  }

  def getVector :Array[Int] = for((i,j) <- vector.zipWithIndex) yield {
    vector(j)
  }

  def hasNext: Boolean = !end

  def next(): Array[String] = {
    inc()
    for((i,p) <- getVector.zipWithIndex) yield values(i)(p)
  }
}
