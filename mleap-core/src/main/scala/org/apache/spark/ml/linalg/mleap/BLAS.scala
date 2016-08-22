package org.apache.spark.ml.linalg.mleap

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg

/**
  * Created by hollinwilkins on 8/22/16.
  */
object BLAS {
  def dot(v1: Vector, v2: Vector): Double = linalg.BLAS.dot(v1, v2)
}
