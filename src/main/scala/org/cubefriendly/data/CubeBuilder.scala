package org.cubefriendly.data

import org.cubefriendly.engine.cube.{CubeDataBuilder, CubeData}
import org.mapdb.{DBMaker, DB}

import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */
class CubeBuilder(val db:DB, cubeDataBuilder:CubeDataBuilder) {
  import scala.collection.JavaConversions._
  private val header:mutable.Buffer[String] = mutable.Buffer()
  private val metrics:mutable.Buffer[String] = mutable.Buffer()
  private val dimSize:mutable.HashMap[String,Int] = mutable.HashMap()

  def name(name:String):CubeBuilder = {
    cubeDataBuilder.name(name)
    this
  }

  def record(record: Vector[String]):CubeBuilder = {
    val vector = header.filter(dim => !metrics.contains(dim)).zipWithIndex.map({case (dim,index) =>
      val indexed = db.createTreeMap("index_" + index).makeOrGet[String,Int]()
      if(!indexed.containsKey(record(index))){
        val size = dimSize.getOrElse(dim,1)
        val inversedIndex = db.createTreeMap("inversed_index_" + index).makeOrGet[Int,String]()
        inversedIndex.put(size,record(index))
        indexed.put(record(index),size)
        dimSize.put(dim, size + 1)
      }
      indexed.get(record(index)):Integer
    }).toList

    cubeDataBuilder.add(vector,metrics)
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header.clear()
    this.header.append(header :_*)
    this
  }

  def addMetric(name:String) = {
    this.metrics.append(name)
  }

  def toCube(name:String):Cube = Cube(name,header.toVector,db,cubeDataBuilder.build())
}

