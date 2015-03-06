package org.cubefriendly.data

import java.io.File

import org.cubefriendly.engine.cube.CubeData
import org.mapdb.{DBMaker, DB}

import scala.io.Source

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */

object Cube {
  import scala.collection.JavaConversions._



  def fromCsv(csv: File, db: DB) = {
    val cubeBuilder = new CubeBuilder(db,CubeData.builder(db))
    val lines = Source.fromFile(csv).getLines()
    val header = lines.next().split(";").toVector
    var counter = 0
    var timestamp = System.currentTimeMillis()
    cubeBuilder.header(header)
    lines.foreach(line => {
      counter = counter + 1
      cubeBuilder.record(line.split(";").toVector)
      if(counter % 1000 == 0){
        println(counter+";" + (System.currentTimeMillis() - timestamp))
        timestamp = System.currentTimeMillis()
      }
    })
    cubeBuilder
  }

  def builder(db:DB) : CubeBuilder = new CubeBuilder(db, CubeData.builder(db))
}

case class Cube(name:String, header:Vector[String], db:DB, cubeData:CubeData) {
  import scala.collection.JavaConversions._
  def dimensions(name: String):Dimension = {
    header.indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i => Dimension(name,db.getTreeMap[Int,String]("inversed_index_" + i).values().iterator())
    }
  }

  def query(where: Map[String, Vector[String]]):Vector[Vector[String]] = {
    val q = where.map(e => {
      val index:Integer = header.indexOf(e._1)
      val idx = db.getTreeMap[String,Integer]("index_" + index)
      index -> seqAsJavaList(e._2.map(idx.get).toSeq)
    }).toMap
    val result = cubeData.query(mapAsJavaMap(q)).map(v => v.zipWithIndex.map({case(value,index) =>
      val inv = db.getTreeMap[Integer,String]("inversed_index_" + index)
      inv.get(value)
    }).toVector).toVector

    result
  }
}

case class Dimension(name:String, values:Iterator[String])