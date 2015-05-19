package org.cubefriendly.data

import java.io.File

import org.cubefriendly.CubefriendlyException
import org.cubefriendly.engine.cube.CubeData
import org.cubefriendly.reflection.Aggregator
import org.mapdb.{DBMaker, DB}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */

object Cube {

  def builder(db:DB) : CubeBuilder = new CubeBuilder(db, CubeData.builder(db))

  def open(file:File):Cube = {
    val db = DBMaker.fileDB(file).make()

    val metaString = db.treeMap[String,String]("meta_string")
    val metaVecString = db.treeMap[String,Vector[String]]("meta_vec_string")

    Cube(metaString.get("name"),metaVecString.get("header"),metaVecString.get("metrics"),db,CubeData.builder(db).build())
  }
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
    dimensions.foreach(dim => groupByValues.append(cube.header.indexOf(dim)))
    this
  }

  def reduce(by: (String, String)*): QueryBuilder = {
    by.foreach({
      case (key,value) if cube.header.indexOf(key) > -1 => reduceValues.put(cube.header.indexOf(key), value)
      case (key,value) if cube.metrics.indexOf(key) > -1 => reduceMetrics.put(cube.metrics.indexOf(key) , value)
      case (key,value) => throw new CubefriendlyException("dimension / metric not found \"" + key + "\"")
    })
    this
  }
  private def validateAggregation() = {
    if(groupByValues.size + reduceValues.size > 0){
      val setMoreThanOnce = (0 until cube.header.size).filter(h => groupByValues.contains(h) && reduceValues.keys.contains(h))

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
      ((0 until cube.header.length).filter(idx => reduceValues.contains(idx) || groupByValues.contains(idx)).map(idx =>
        if(groupByValues.contains(idx)){
          key.find({case(index,_) => index == idx}) match {
            case Some((i,v)) => v
            case None => throw new CubefriendlyException("Serious bug while aggregating value. Trying to get a value that does not exist!")
          }
        }else {
          dims(idx).finish
        }
      ).toVector,
      (0 until cube.metrics.length).map(idx =>
        metrics(idx).finish
      ).toVector)
    }).toIterator
  }

  def run(): Iterator[(Vector[String],Vector[String])] = {
    validateAggregation()

    val q = selectedValues.map({ case (key, values) =>
      val index: Integer = cube.header.indexOf(key)
      val idx = cube.db.treeMap[String, Integer]("index_" + index)
      index -> seqAsJavaList(values.map(idx.get).toSeq)
    }).toMap
    val result = cube.cubeData.query(mapAsJavaMap(q)).map(v =>
      (v.vector.zipWithIndex.map({case(value,index) =>
      val inv = cube.db.treeMap[Integer,String]("inversed_index_" + index)
      inv.get(value)
    }).toVector,v.metrics.toVector))

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

case class Cube(name:String, header:Vector[String], metrics:Vector[String], db:DB, cubeData:CubeData) {
  def close():Unit = db.close()

  def dimensions(name: String):Dimension = {
    header.indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i => Dimension(name,db.treeMap[Int,String]("inversed_index_" + i).values().iterator())
    }
  }
}

case class Dimension(name:String, values:Iterator[String])

