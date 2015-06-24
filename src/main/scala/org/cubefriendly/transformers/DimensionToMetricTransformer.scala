package org.cubefriendly.transformers

import org.cubefriendly.data.Cube

/**
 * Created by davidroon on 18.06.15.
 */
class DimensionToMetricTransformer {
 def transform(source:Cube, metrics:Vector[String]) :Cube = {
   metrics.foreach(source.toMetric)
   source
 }
}
