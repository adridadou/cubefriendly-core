package org.cubefriendly.data

import org.cubefriendly.engine.cube.{CubeDataBuilder, CubeData}
import org.mapdb.DB

import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */
class CubeBuilder(val db:DB, cubeDataBuilder:CubeDataBuilder) {
  import scala.collection.JavaConversions._
  private var header:Vector[String] = Vector()
  private val dimSize:mutable.HashMap[String,Int] = mutable.HashMap()

  def name(name:String) = {
    cubeDataBuilder.name(name)
    this
  }

  def record(record: Vector[String]):CubeBuilder = {
    header.zipWithIndex.foreach({case (dim,index) =>
      val indexed = db.createTreeMap("index_" + index).makeOrGet[String,Int]()
      val inversedIndex = db.createTreeMap("inversed_index_" + index).makeOrGet[Int,String]()
      if(!indexed.containsKey(record(index))){
        val size = dimSize.getOrElse(dim,0)
        inversedIndex.put(size,record(index))
        indexed.put(record(index),size)
        dimSize.put(dim, size + 1)
      }
      val vector = record.map(indexed.get(_):Integer)
      cubeDataBuilder.add(vector)
    })
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header = header
    this
  }
  def toCube(name:String) = Cube(name,header,db,cubeDataBuilder.build())
}

