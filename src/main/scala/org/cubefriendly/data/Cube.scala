package org.cubefriendly.data

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.engine.cube.CubeData
import org.cubefriendly.processors.Language
import org.cubefriendly.reflection.Aggregator
import org.mapdb.{BTreeMap, DB, DBMaker}

import scala.collection.JavaConversions._
import scala.collection.mutable

import scalacache._
import lrumap._

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 * This code is released under Apache 2 license
 */

object Cube {
  def builder(file: File): CubeBuilder = {
    val db = createDb(file)
    new CubeBuilder(MapDbInternal(db), CubeData.builder(db))
  }

  def open(file:File):Cube = {
    val db = createDb(file)
    Cube(MapDbInternal(db), CubeData.builder(db).build())
  }

  def map(db: DB, mapType: IndexInv): BTreeMap[Integer, String] = db.treeMap[Integer, String](mapType.name)

  def map(db: DB, mapType: Index): BTreeMap[String, Integer] = db.treeMap[String, Integer](mapType.name)

  private def createDb(file: File): DB = DBMaker.fileDB(file)
    .transactionDisable()
    .asyncWriteEnable()
    .fileMmapEnableIfSupported()
    .make()
}

abstract sealed class MapType(val name: String)

case class Index(index: Int) extends MapType("index_" + index)

case class IndexInv(index: Int) extends MapType("inversed_index_" + index)

case class CodesToValues(index:Int, lang:Language) extends MapType("c2v_" + index + "_" + lang.code)

case class ValuesToCodes(index:Int, lang:Language) extends MapType("v2c_" + index + "_" + lang.code)

case object Meta extends MapType("meta_string")

case object MetaList extends MapType("meta_vec_string")

trait DataInternals {
  def map(t: Index): mutable.Map[String, Integer] = getMap[String, Integer](t)

  def map(t: IndexInv): mutable.Map[Integer, String] = getMap[Integer, String](t)

  def map(t: Meta.type): mutable.Map[String, String] = getMap[String, String](t)

  def map(t: MetaList.type): mutable.Map[String, Vector[String]] = getMap[String, Vector[String]](t)

  def map(t:ValuesToCodes) : mutable.Map[String,String] = getMap[String,String](t)

  def map(t:CodesToValues) : mutable.Map[String,String] = getMap[String,String](t)

  def getMap[K, V](t: MapType): mutable.Map[K, V]

  def deleteMap(t:MapType):Unit

  def commit(): Unit

  def close(): Unit
}

case class MapDbInternal(db: DB) extends DataInternals {
  def getMap[K, V](t: MapType): mutable.Map[K, V] = db.treeMapCreate(t.name).makeOrGet[K, V]()

  def deleteMap(t:MapType):Unit = db.delete(t.name)

  def commit(): Unit = db.commit()

  def close(): Unit = {
    db.commit()
    db.close()
  }
}

class QueryBuilder(val cube:Cube) {

  implicit val scalaCache = QueryBuilder.scalaCache

  private val selectedValues: mutable.HashMap[String, Vector[String]] = mutable.HashMap()
  private val eliminateValues: mutable.Set[Int] = mutable.HashSet()
  private val reduceMetrics:mutable.HashMap[Int,String] = mutable.HashMap()
  private var lang:Option[Language] = None

  cube.metrics().foreach(metric => reduce(metric -> "sum"))

  def in(lang:Language) : QueryBuilder = {
    this.lang = Some(lang)
    this
  }

  def toDefaultDimension(lang:Language, dimension:String) :String = {
    val dimensionsInLang = cube.dimensions(lang)
    dimensionsInLang.indexOf(dimension) match {
      case -1 => throw new CubefriendlyException("cannot find dimension " + dimension + " in " + lang.code)
      case index => cube.dimensions()(index)
    }
  }

  def where(lang: Language, tuple: (String, Vector[String])*):QueryBuilder = where(lang,tuple.toMap)

  def where(where: (String, Vector[String])*): QueryBuilder = this.where(where.toMap)

  def where(lang:Language, where:Map[String, Vector[String]]): QueryBuilder = {
    val dimensions = cube.dimensions(lang)
    val defaultDimensions = cube.dimensions()

    val newWhere:Map[String,Vector[String]] = where.map({case (key,values) =>
      dimensions.indexOf(key) match {
        case -1 => throw new CubefriendlyException("dimension " + key + " cannot be found in " + lang.code + " " + dimensions)
        case i =>
          val mapping = cube.internal.map(ValuesToCodes(i,lang))
          defaultDimensions(i) -> values.flatMap(mapping.get)
      }
    })
    this.where(newWhere)
  }

  def where(where: Map[String, Vector[String]]): QueryBuilder = {
    where.foreach({ case (key, value) =>
      val values = selectedValues.getOrElse(key, Vector[String]())
      selectedValues.put(key, values ++ value)
    })
    this
  }

  def eliminate(lang:Language, dimensions:String*) : QueryBuilder = eliminate(dimensions.map(toDefaultDimension(lang,_)) :_*)

  def eliminate(dimensions:String*): QueryBuilder = {
    dimensions.foreach(dim => eliminateValues.add(cube.dimensions().indexOf(dim)))
    this
  }

  def reduce(lang:Language, by: (String, String)*): QueryBuilder = {reduce(by.map{case (key,value) => toDefaultDimension(lang,key) -> value} :_*)
  }

  def reduce(by: (String, String)*): QueryBuilder = {
    by.foreach({
      case (key, value) if cube.metrics().indexOf(key) > -1 => reduceMetrics.put(cube.metrics().indexOf(key), value)
      case (key,value) => throw new CubefriendlyException("metric not found \"" + key + "\"")
    })
    this
  }

  private def searchKey():String = {
    val value = selectedValues.toString() + reduceMetrics.toString() + eliminateValues.toString()
    val digest = MD5.messageDigest
    val bytes = digest.digest(value.getBytes)
    val key = new String(bytes)
    key
  }

  private def aggregate(result:Iterator[(Vector[String],Vector[String])]): Vector[(Vector[String],Vector[String])] = caching(cube.name() + searchKey()) {
    val aggregatedResult = mutable.Map.empty[IndexedSeq[String],Array[Aggregator]]

    val cubeMetrics = cube.metrics()
    val metricIndices = cubeMetrics.indices
    val nbMetrics = cubeMetrics.length
    val usefulIndexes = cube.dimensions().indices.filter(idx => !eliminateValues.contains(idx))

    result.foreach({case (record,metrics) =>
      val newKey = usefulIndexes.map(record.apply)

      val aggregationMetrics = aggregatedResult.get(newKey) match {
        case Some(vector) => vector
        case None =>
          val aggregators:Array[Aggregator] = new Array(nbMetrics)
          metricIndices.foreach(idx => aggregators(idx) = Aggregator.funcs(reduceMetrics.getOrElse(idx,"sum")))
          aggregatedResult(newKey) = aggregators
          aggregators
      }
      metricIndices.foreach(idx => {aggregationMetrics(idx).reduce(metrics(idx))
      })
    })

    aggregatedResult.keys.map(key =>
      (key.toVector,
        cubeMetrics.indices.map(idx =>
          aggregatedResult(key)(idx).finish.toString
      ).toVector
    )).toVector
  }

  def run(): Iterator[(Vector[String],Vector[String])] = {

    val q = selectedValues.map({ case (key, values) =>
      val index: Integer = cube.dimensions().indexOf(key)
      val idx = cube.internal.map(Index(index))
      index -> seqAsJavaList(values.map(idx.apply).toSeq)
    }).toMap
    val dimensionIndexes = cube.dimensions().indices
    val indexes = dimensionIndexes.map(index => cube.internal.map(IndexInv(index))).toVector
    val result = cube.cubeData.query(mapAsJavaMap(q)).map(v =>
      (v.vector.zipWithIndex.map({ case (value, index) => indexes(index)(value)
      }).toVector, v.metrics.toVector))

    val localeResult:Iterator[(Vector[String], Vector[String])] = lang.map(lang => {
      val codes2values:Vector[Map[String, String]] = cube.dimensions().indices.map(index => cube.internal.map(CodesToValues(index, lang)).toMap).toVector
      translateResult(result,codes2values, lang)
    }).getOrElse(result)

    if(eliminateValues.nonEmpty){
      aggregate(localeResult).toIterator
    }else {
      localeResult
    }
  }

  private def translateResult(result:Iterator[(Vector[String], Vector[String])],codes2values:Vector[Map[String, String]], lang:Language):Iterator[(Vector[String], Vector[String])] = {
    result.map({case (vector,metrics) =>
      val result = for ((entry, index) <- vector.zipWithIndex) yield {
        codes2values(index)(entry)
      }
      (result,metrics)
    })
  }
}

object QueryBuilder {

  val scalaCache = ScalaCache(LruMapCache(100))

  def query(cube:Cube):QueryBuilder = new QueryBuilder(cube)
}

case class Cube(internal: DataInternals, cubeData: CubeData) {
  private lazy val metaString = internal.map(Meta)
  private lazy val metaVecString = internal.map(MetaList)

  def dimensions(): Vector[String] = metaVecString(MetaDimensions.name)

  def dimensions(lang:Language): Vector[String] = metaVecString("dimensions_" + lang.code)

  def metrics(): Vector[String] = metaVecString(MetaMetrics.name)

  def name(): String = metaString("name")

  def close(): Unit = internal.close()

  def toMetric(dimension:String):Cube = {
    dimensions().indexOf(dimension) match {
      case -1 => this
      case i =>
        cubeData.toMetric(i, internal.map(IndexInv(i)))
        internal.deleteMap(Index(i))
        internal.deleteMap(IndexInv(i))
        metaVecString.put(MetaDimensions.name,dimensions().filter(h => h != dimension))
        metaVecString.put(MetaMetrics.name,metrics() ++ Vector(dimension))
        internal.commit()
        this
    }
  }

  def dimension(name: String): Dimension = {
    dimensions().indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i =>
        val values = internal.map(IndexInv(i))
        val vec = (1 to values.size).map(index => values(index)).toVector
        Dimension(name, vec)
    }
  }
}

abstract sealed class MetaType(val name: String)

case object MetaName extends MetaType("name")

abstract sealed class MetaListType(val name: String)

case object MetaDimensions extends MetaListType("dimensions")

case class MetaLangSpecificDimensions(lang:Language) extends MetaListType("dimensions_" + lang.code)

case object MetaMetrics extends MetaListType("metrics")

case class Dimension(name:String, values:Vector[String])

