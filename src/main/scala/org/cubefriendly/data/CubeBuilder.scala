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
  private val dimSize:mutable.HashMap[String,Int] = mutable.HashMap()

  def name(name:String) = {
    cubeDataBuilder.name(name)
    this
  }

  def record(record: Vector[String]):CubeBuilder = {
    val vector = header.zipWithIndex.map({case (dim,index) =>
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

    cubeDataBuilder.add(vector)
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header.clear()
    this.header.append(header)
    this
  }
  def toCube(name:String) = Cube(name,header,db,cubeDataBuilder.build())
}

