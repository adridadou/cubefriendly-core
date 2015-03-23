package org.cubefriendly.data

import java.io.File

import org.cubefriendly.engine.cube.{CubeData, CubeDataBuilder}
import org.mapdb.DB
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */
class CubeBuilder(val db:DB, cubeDataBuilder:CubeDataBuilder) {

  private val header:mutable.Buffer[String] = mutable.Buffer()
  private val metrics:mutable.Buffer[String] = mutable.Buffer()
  private val dimSize:mutable.HashMap[String,Int] = mutable.HashMap()
  private var source:Option[File] = None
  private var csvSeparator = ""

  def metrics(metricList: String*):CubeBuilder = {
    metrics.appendAll(metricList)

    val metaVecString = db.getTreeMap[String,Vector[String]]("meta_vec_string")
    metaVecString.put("metrics",this.metrics.toVector)
    db.commit()

    this
  }

  def csv(file: File, separator: String):CubeBuilder = {
    source = Some(file)
    csvSeparator = separator
    this
  }

  def name(name:String):CubeBuilder = {
    cubeDataBuilder.name(name)
    val metaString = db.getTreeMap[String,String]("meta_string")
    metaString.put("name",name)
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
    })

    cubeDataBuilder.add(vector,metrics)
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header.clear()
    this.header.append(header :_*)
    val metaVecString = db.getTreeMap[String,Vector[String]]("meta_vec_string")
    metaVecString.put("header",this.header.toVector)
    db.commit()
    this
  }

  def toCube(name:String):Cube = {
    source.foreach(csv => {
      val lines = Source.fromFile(csv).getLines()
      header(lines.next().split(csvSeparator).toVector)
      lines.filter(_.nonEmpty).foreach(line => record(line.split(csvSeparator).toVector))
    })

    Cube(name,header.toVector,metrics.toVector,db,cubeDataBuilder.build())
  }
}

