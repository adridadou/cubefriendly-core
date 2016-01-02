package org.cubefriendly.reflection

import org.cubefriendly.CubefriendlyException

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


/**
 * Cubefriendly
 * Created by david on 12.03.15.
 */

object Reflection {
  lazy val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
}

trait FunctionsHolder[T] {
  val _funcs:mutable.HashMap[String,() => T] = mutable.HashMap()

  def newFunc(tree:Tree):() => T = Reflection.tb.eval(tree).asInstanceOf[() => T]

  def funcs(name:String) : T = {
    _funcs.get(name) match {
      case Some(f) => f()
      case None => throw new CubefriendlyException("no function " + name + " found. available:" + _funcs.keys.toSeq)
    }
  }

  def build(tree:Tree) : T = {
    Reflection.tb.eval(tree).asInstanceOf[() => T]()
  }
}
