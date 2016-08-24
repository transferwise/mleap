package com.truecar.mleap.core.feature

import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
 * Created by hwilkins on 11/5/15.
 */
object VectorAssemblerModel {
  val default: VectorAssemblerModel = VectorAssemblerModel()
}

case class VectorAssemblerModel() extends Serializable {
  def apply(vv: Any *): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
    }
    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }
}
