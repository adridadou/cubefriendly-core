package org.cubefriendly.data


import org.cubefriendly.engine.cube.CubeDataBuilder
import org.cubefriendly.processors.Language

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Cubefriendly
 * Created by david on 23.02.15.
 * This code is released under Apache 2 license
 */
class CubeBuilder(internal: DataInternals, cubeDataBuilder: CubeDataBuilder) {

  private val dimensions:mutable.Buffer[String] = mutable.Buffer()
  private val langDimensions:mutable.Map[Language,Vector[String]] = new mutable.HashMap()
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

  def prepareCodes(codes:Vector[Vector[String]]):CubeBuilder = {
    codes.indices.foreach(i => {
      val indexMap = internal.map(Index(i))
      val inversedIndexMap = internal.map(IndexInv(i))
      codes(i).indices.foreach(j => {
        val code = codes(i)(j)
        indexMap.put(code,j + 1)
        inversedIndexMap.put(j + 1,code)
        dimSize.put(dimensions(i), codes(i).size)
      })
    })
    this
  }

  def record(record: Vector[Int], metric:String):CubeBuilder = {
    val vector:Vector[Integer] = record.map(i => i:java.lang.Integer)
    cubeDataBuilder.add(vector,Vector(metric))
    this
  }

  def record(record: Vector[String]):CubeBuilder = {
    val vector: Vector[Integer] = dimensions.filter(dim => !metrics.contains(dim)).zipWithIndex.map({ case (dim, index) =>
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

  def dimensions(header:Vector[String], lang:Language):CubeBuilder = {
    langDimensions.put(lang,header)
    this
  }

  def dimensions(header:Vector[String]):CubeBuilder = {
    this.dimensions.clear()
    this.dimensions.append(header :_*)
    this
  }

  def dimensions(lang:Language, data:Vector[Vector[String]]):CubeBuilder = {
    data.indices.foreach(index =>{
      val values2codes = internal.map(ValuesToCodes(index,lang))
      val codes2values = internal.map(CodesToValues(index,lang))
      val values = data(index)
      val indexInvMap = internal.map(IndexInv(index))
      values.indices.foreach(j => {
        val code = indexInvMap(j + 1)
        val value = values(j)
        values2codes.put(value,code)
        codes2values.put(code,value)
      })
    })

    this
  }

  def toCube:Cube = {
    val cube = Cube(internal, cubeDataBuilder.build())
    meta(MetaDimensions, dimensions.toVector)
    meta(MetaMetrics, metrics.toVector)

    langDimensions.foreach({case(lang,values) =>
      meta(MetaLangSpecificDimensions(lang),values)
    })

    cube
  }

  def meta(key: MetaListType, value: Vector[String]): Unit = internal.map(MetaList).put(key.name, value)
}
