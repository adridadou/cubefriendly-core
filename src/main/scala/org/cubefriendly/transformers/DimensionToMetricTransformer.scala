package org.cubefriendly.transformers

import org.cubefriendly.data.Cube

/**
 * Cubefriendly
 * Created by davidroon on 18.06.15.
 * This code is released under Apache 2 license
 */
class DimensionToMetricTransformer {
 def transform(source:Cube, metrics:Vector[String]) :Cube = {
   metrics.foreach(source.toMetric)
   source
 }
}
