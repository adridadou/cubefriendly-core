package org.cubefriendly.data
import org.cubefriendly.engine.cube.CubeData
import org.mapdb.DB

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */

object Cube {
  def builder(db:DB) : CubeBuilder = new CubeBuilder(db, CubeData.builder(db))
}

case class Cube(name:String, header:Vector[String], db:DB, cubeData:CubeData) {
  import scala.collection.JavaConversions._
  def dimensions(name: String):Dimension = {
    header.indexOf(name) match {
      case -1 => throw new NoSuchElementException("no dimension " + name)
      case i => Dimension(name,db.getTreeMap[Int,String]("inversed_index_" + i).values().toVector)
    }
  }

  def query(where: Map[String, Vector[String]]):Vector[Vector[String]] = {
    val q = where.map(e => {
      val index:Integer = header.indexOf(e._1)
      val idx = db.getTreeMap[String,Integer]("index_" + index)
      index -> seqAsJavaList(e._2.map(idx.get).toSeq)
    }).toMap
    cubeData.query(mapAsJavaMap(q)).map(v => v.zipWithIndex.map(e => {
      val inv = db.getTreeMap[Integer,String]("inversed_index_" + e._2)
      inv.get(e._1)
    }).toVector).toVector
  }
}

case class Dimension(name:String, values:Vector[String])