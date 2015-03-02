package org.cubefriendly.data

import org.cubefriendly.engine.cube.{CubeDataBuilder, CubeData}
import org.mapdb.DB

/**
 * Created by david on 23.02.15.
 */
class CubeBuilder(val db:DB, cubeDataBuilder:CubeDataBuilder) {
  import scala.collection.JavaConversions._
  private var header:Vector[String] = Vector()

  def name(name:String) = {
    cubeDataBuilder.name(name)
    this
  }

  def record(record: Vector[String]):CubeBuilder = {
    header.zipWithIndex.foreach({case (dim,index) => {
      val indexed = db.createTreeMap("index_" + index).makeOrGet[String,Int]()
      val inversedIndex = db.createTreeMap("inversed_index_" + index).makeOrGet[Int,String]()
      if(!indexed.containsKey(record(index))){
        inversedIndex.put(inversedIndex.size(),record(index))
        indexed.put(record(index),indexed.size())
        db.commit()
      }
      val vector = record.map(indexed.get(_):Integer).toList
      cubeDataBuilder.add(vector)
    }})
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header = header
    this
  }
  def toCube(name:String) = Cube(name,header,db,cubeDataBuilder.build())
}

