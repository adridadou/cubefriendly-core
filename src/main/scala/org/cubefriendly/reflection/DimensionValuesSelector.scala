package org.cubefriendly.reflection

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import com.googlecode.concurrenttrees.suffix.{ConcurrentSuffixTree, SuffixTree}
import org.cubefriendly.CubefriendlyException
import org.cubefriendly.data.{Index, Cube}
import org.cubefriendly.processors.Language
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scalacache._
import lrumap._

/**
 * Cubefriendly
 * Created by davidroon on 01.10.15.
 * This code is released under Apache 2 license
 */
trait DimensionValuesSelector {

  implicit val scalaCache = DimensionValuesSelector.scalaCache

  def select(cube:Cube, dimension:String, lang:Option[Language], args: Map[String,String]):Iterator[String]
  def suffixIndex(cube:Cube, dimension:String, lang:Option[Language]):SuffixTree[String] = caching(cube.name() + "#" + dimension + "#" + lang.map("#" + _.code).getOrElse("")) {
    val tree = new ConcurrentSuffixTree[String](new DefaultCharArrayNodeFactory())
    val values = cube.dimensions(lang).indexOf(dimension) match {
      case i if i > -1 => cube.internal.map(Index(2)).keys
      case -1 => throw new CubefriendlyException("dimension " + dimension + " cannot be found")
     }

    values.foreach(value => tree.put(value.toLowerCase,value))
    tree
  }
}

object DimensionValuesSelector extends FunctionsHolder[DimensionValuesSelector]{

  val cacheSize = 100

  val scalaCache = ScalaCache(LruMapCache(cacheSize))

  def registerSearch(): Unit = {
    val select = """value.contains(args("term"))"""
    val bestResult = """value.equalsIgnoreCase(args("term"))"""
    register("search", select, bestResult)
  }

  private def parse(select:String, bestResult:String) : Tree = {
    val fun =
      s"""
         |import org.cubefriendly.data.Cube
         |import org.cubefriendly.processors.Language
         |import scala.collection.JavaConversions._
         |() => new org.cubefriendly.reflection.DimensionValuesSelector {
         | def select(cube: org.cubefriendly.data.Cube, dimension: String, lang: Option[Language], args: Map[String, String]): Iterator[String] = {
         |    val index = suffixIndex(cube,dimension,lang)
         |    val term = args("term")
         |    val found = Option(index.getValueForExactKey(term))
         |    val result = index.getKeysContaining(term).iterator()
         |
         |    found.toIterator ++ result.map(_.toString)
         |  }}
      """.stripMargin

    Reflection.tb.parse(fun)
  }
  def register(name:String, select:String, bestResult:String):Unit = {
    val func = parse(select, bestResult)
    _funcs.put(name,newFunc(func))
  }

  registerSearch()

}


class Top extends DimensionValuesSelector{
  override def select(cube: Cube, dimension: String, lang: Option[Language], args: Map[String, String]): Iterator[String] = {
    val index = suffixIndex(cube,dimension,lang)
    val term = args("term")
    val found = Option(index.getValueForExactKey(term))
    val result = index.getKeysContaining(term).iterator()

    found.toIterator ++ result.map(_.toString)
  }
}
