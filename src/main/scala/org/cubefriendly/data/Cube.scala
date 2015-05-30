package org.cubefriendly.data

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.engine.cube.CubeData
import org.cubefriendly.reflection.Aggregator
import org.mapdb.{BTreeMap, DB, DBMaker}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
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

  private def createDb(file: File): DB = DBMaker.fileDB(file).compressionEnable().make()
}

abstract sealed class MapType(val name: String)

case class Index(index: Int) extends MapType("index_" + index)

case class IndexInv(index: Int) extends MapType("inversed_index_" + index)

case object Meta extends MapType("meta_string")

case object MetaList extends MapType("meta_vec_string")

trait DataInternals {
  def map(t: Index): mutable.Map[String, Integer] = getMap[String, Integer](t)

  def map(t: IndexInv): mutable.Map[Integer, String] = getMap[Integer, String](t)

  def map(t: Meta.type): mutable.Map[String, String] = getMap[String, String](t)

  def map(t: MetaList.type): mutable.Map[String, Vector[String]] = getMap[String, Vector[String]](t)

  def getMap[K, V](t: MapType): mutable.Map[K, V]

  def commit(): Unit

  def close(): Unit
}

case class MapDbInternal(db: DB) extends DataInternals {
  def getMap[K, V](t: MapType): mutable.Map[K, V] = db.treeMap[K, V](t.name)

  def commit() = db.commit()

  def close() = db.close()
}

class QueryBuilder(val cube:Cube) {
  private val selectedValues: mutable.HashMap[String, Vector[String]] = mutable.HashMap()
  private val groupByValues: mutable.Buffer[Int] = mutable.Buffer()
  private val reduceValues:mutable.HashMap[Int,String] = mutable.HashMap()
  private val reduceMetrics:mutable.HashMap[Int,String] = mutable.HashMap()

  def where(where: (String, Vector[String])*): QueryBuilder = this.where(where.toMap)

  def where(where: Map[String, Vector[String]]): QueryBuilder = {
    where.foreach({ case (key, value) =>
      val values = selectedValues.getOrElse(key, Vector[String]())
      selectedValues.put(key, values ++ value)
    })
    this
  }

  def groupBy(dimensions:String*): QueryBuilder = {
    dimensions.foreach(dim => groupByValues.append(cube.header().indexOf(dim)))
    this
  }

  def reduce(by: (String, String)*): QueryBuilder = {
    by.foreach({
      case (key, value) if cube.header().indexOf(key) > -1 => reduceValues.put(cube.header().indexOf(key), value)
      case (key, value) if cube.metrics().indexOf(key) > -1 => reduceMetrics.put(cube.metrics().indexOf(key), value)
      case (key,value) => throw new CubefriendlyException("dimension / metric not found \"" + key + "\"")
    })
    this
  }
  private def validateAggregation() = {
    if(groupByValues.size + reduceValues.size > 0){
      val setMoreThanOnce = cube.header().indices.filter(h => groupByValues.contains(h) && reduceValues.keys.contains(h))

      if(setMoreThanOnce.nonEmpty){
        throw new CubefriendlyException("you cannot set a dimension for group by and reduce at the same time:" + setMoreThanOnce)
      }
    }
  }

  private def aggregate(result:Iterator[(Vector[String],Vector[String])]): Iterator[(Vector[String],Vector[String])] = {
    val aggregatedResult = mutable.HashMap[Vector[(Int,String)],(mutable.HashMap[Int,Aggregator],mutable.HashMap[Int,Aggregator])]()
    result.foreach({case (record,metrics) =>
      val newKey = record.zipWithIndex.filter({case(elem,index) => groupByValues.contains(index)}).map({case (elem,index) => (index,record.get(index))})
      val (aggregationMap,aggregationMetricsMap) = aggregatedResult.getOrElseUpdate(newKey,(mutable.HashMap(),mutable.HashMap()))
      reduceValues.keys.foreach(idx =>{
        val current:Aggregator = aggregationMap.getOrElseUpdate(idx,Aggregator.newFunc(reduceValues(idx)))
        current.reduce(record(idx))
      })

      reduceMetrics.keys.foreach(idx =>{
        val current:Aggregator = aggregationMetricsMap.getOrElseUpdate(idx,Aggregator.newFunc(reduceMetrics.getOrElse(idx,"sum")))
        current.reduce(metrics(idx))
      })
    })
    aggregatedResult.map({case(key,(dims,metrics)) =>
      (cube.header().indices.filter(idx => reduceValues.contains(idx) || groupByValues.contains(idx)).map(idx =>
        if (groupByValues.contains(idx)) {
          key.find({case(index,_) => index == idx}) match {
            case Some((i,v)) => v
            case None => throw new CubefriendlyException("Serious bug while aggregating value. Trying to get a value that does not exist!")
          }
        }else {
          dims(idx).finish
        }
      ).toVector,
        cube.metrics().indices.map(idx =>
          metrics(idx).finish
      ).toVector)
    }).toIterator
  }

  def run(): Iterator[(Vector[String],Vector[String])] = {
    validateAggregation()

    val q = selectedValues.map({ case (key, values) =>
      val index: Integer = cube.header().indexOf(key)
      val idx = cube.internal.map(Index(index))
      index -> seqAsJavaList(values.map(idx.apply).toSeq)
    }).toMap
    val result = cube.cubeData.query(mapAsJavaMap(q)).map(v =>
      (v.vector.zipWithIndex.map({ case (value, index) => cube.internal.map(IndexInv(index))(value)
      }).toVector, v.metrics.toVector))

    if(groupByValues.nonEmpty || reduceValues.nonEmpty){
      aggregate(result)
    }else {
      result
    }
  }
}

object QueryBuilder {
  def query(cube:Cube):QueryBuilder = new QueryBuilder(cube)
}

case class Cube(internal: DataInternals, cubeData: CubeData) {
  private lazy val metaString = internal.map(Meta)
  private lazy val metaVecString = internal.map(MetaList)

  def header(): Vector[String] = metaVecString("header")

  def metrics(): Vector[String] = metaVecString("metrics")

  def name(): String = metaString("name")

  def meta(key: MetaType): String = metaString(key.name)

  def meta(key: MetaListType): Vector[String] = metaVecString(key.name)

  def close(): Unit = internal.close()

  def dimensions(name: String):Dimension = {
    header().indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i =>
        val values = internal.map(IndexInv(i))
        val it = (1 to values.size).iterator.map(index => values(index))
        Dimension(name, it)
    }
  }
}

abstract sealed class MetaType(val name: String)

case object MetaName extends MetaType("name")

abstract sealed class MetaListType(val name: String)

case object MetaHeader extends MetaListType("header")

case object MetaMetrics extends MetaListType("metrics")

case class Dimension(name:String, values:Iterator[String])

