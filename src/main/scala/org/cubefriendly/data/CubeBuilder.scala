package org.cubefriendly.data


import org.cubefriendly.engine.cube.CubeDataBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 */
class CubeBuilder(internal: DataInternals, cubeDataBuilder: CubeDataBuilder) {

  private val header:mutable.Buffer[String] = mutable.Buffer()
  private val metrics:mutable.Buffer[String] = mutable.Buffer()
  private val dimSize:mutable.HashMap[String,Int] = mutable.HashMap()

  def metrics(metricList: String*):CubeBuilder = {
    metrics.appendAll(metricList)
    this
  }

  def name(name:String):CubeBuilder = {
    meta(MetaName, name)
    this
  }

  def meta(key: MetaType, value: String): Unit = internal.map(Meta).put(key.name, value)

  def record(record: Vector[String]):CubeBuilder = {
    val vector: Vector[Integer] = header.filter(dim => !metrics.contains(dim)).zipWithIndex.map({ case (dim, index) =>
      val indexed = internal.map(Index(index))

      if(!indexed.containsKey(record(index))){
        val size = dimSize.getOrElse(dim,1)
        val indexInv = internal.map(IndexInv(index))
        indexInv.put(size, record(index))
        indexed.put(record(index),size)
        dimSize.put(dim, size + 1)
      }
      indexed(record(index))
    }).toVector

    cubeDataBuilder.add(vector,metrics)
    this
  }

  def header(header:Vector[String]):CubeBuilder = {
    this.header.clear()
    this.header.append(header :_*)
    this
  }

  def toCube:Cube = {
    val cube = Cube(internal, cubeDataBuilder.build())
    meta(MetaHeader, header.toVector)
    meta(MetaMetrics, metrics.toVector)
    internal.commit()
    cube
  }

  def meta(key: MetaListType, value: Vector[String]): Unit = internal.map(MetaList).put(key.name, value)
}
