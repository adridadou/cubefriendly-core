package org.cubefriendly.data

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.engine.cube.CubeData
import org.cubefriendly.reflection.Aggregator
import org.mapdb.DB

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */

object Cube {

  def fromCsv(csv: File, db: DB) = {
    val cubeBuilder = new CubeBuilder(db,CubeData.builder(db))
    val lines = Source.fromFile(csv).getLines()
    cubeBuilder.header(lines.next().split(";").toVector)
    lines.foreach(line => cubeBuilder.record(line.split(";").toVector))
    cubeBuilder
  }

  def builder(db:DB) : CubeBuilder = new CubeBuilder(db, CubeData.builder(db))
}

class QueryBuilder(val cube:Cube) {
  private val selectedValues: mutable.HashMap[String, Vector[String]] = mutable.HashMap()
  private val groupByValues: mutable.Buffer[Int] = mutable.Buffer()
  private val reduceValues:mutable.HashMap[Int,String] = mutable.HashMap()

  def where(where: (String, Vector[String])*): QueryBuilder = this.where(where.toMap)

  def where(where: Map[String, Vector[String]]): QueryBuilder = {
    where.foreach({ case (key, value) =>
      val values = selectedValues.getOrElse(key, Vector[String]())
      selectedValues.put(key, values ++ value)
    })
    this
  }

  def groupBy(dimensions:String*): QueryBuilder = {
    dimensions.foreach(dim => groupByValues.append(cube.header.indexOf(dim)))
    this
  }

  def reduce(by: (String, String)*): QueryBuilder = {
    reduceValues.putAll(mapAsJavaMap(by.map({case(key,value) => cube.header.indexOf(key) -> value}).toMap))
    this
  }
  private def validateAggregation() = {
    if(groupByValues.size + reduceValues.size > 0){
      val setMoreThanOnce = (0 until cube.header.size).filter(h => !(groupByValues.contains(h) ^ reduceValues.keys.contains(h)))
      if(setMoreThanOnce.nonEmpty){
        throw new CubefriendlyException("you cannot set a dimension for group by and reduce at the same time:" + setMoreThanOnce)
      }
    }
  }

  def run(): Iterator[Vector[String]] = {

    validateAggregation

    val q = selectedValues.map({ case (key, values) =>
      val index: Integer = cube.header.indexOf(key)
      val idx = cube.db.getTreeMap[String, Integer]("index_" + index)
      index -> seqAsJavaList(values.map(idx.get).toSeq)
    }).toMap
    val result = cube.cubeData.query(mapAsJavaMap(q)).map(v =>
      v.zipWithIndex.map({case(value,index) =>
      val inv = cube.db.getTreeMap[Integer,String]("inversed_index_" + index)
      inv.get(value)
    }).toVector)

    if(groupByValues.nonEmpty || reduceValues.nonEmpty){
      val aggregatedResult = mutable.HashMap[Vector[(Int,String)],mutable.HashMap[Int,Aggregator]]()

      result.foreach(record => {
        val newKey = record.zipWithIndex.filter({case(elem,index) => groupByValues.contains(index)}).map({case (elem,index) => (index,record.get(index))})
        val aggregationMap = aggregatedResult.getOrElseUpdate(newKey,mutable.HashMap())
        reduceValues.keys.foreach(idx =>{
          val current:Aggregator = aggregationMap.getOrElseUpdate(idx,Aggregator.newFunc(reduceValues(idx)))
          current.reduce(record(idx))
        })
      })
      val tmp = aggregatedResult.map({case(key,value) =>
        (0 until cube.header.length).filter(idx => reduceValues.contains(idx) || groupByValues.contains(idx)).map(idx =>
          if(groupByValues.contains(idx)){
            key.find(_._1 == idx) match {
              case Some((i,v)) => v
              case None => throw new CubefriendlyException("Serious bug while aggregating value. Trying to get a value that does not exist!")
            }
          }else {
            value(idx).finish
          }
        ).toVector
      })
      tmp.toIterator
    }else {
      result
    }
  }
}

object QueryBuilder {
  def query(cube:Cube) = new QueryBuilder(cube)
}

case class Cube(name:String, header:Vector[String], db:DB, cubeData:CubeData) {
  import scala.collection.JavaConversions._
  def dimensions(name: String):Dimension = {
    header.indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i => Dimension(name,db.getTreeMap[Int,String]("inversed_index_" + i).values().iterator())
    }
  }
}

case class Dimension(name:String, values:Iterator[String])

