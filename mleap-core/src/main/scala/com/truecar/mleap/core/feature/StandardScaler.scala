package com.truecar.mleap.core.feature

import org.apache.spark.ml.linalg.{Vector, Vectors, SparseVector, DenseVector}

/**
 * Created by hwilkins on 11/5/15.
 */
case class StandardScaler(std: Option[Vector],
                          mean: Option[Vector]) extends Serializable {
  def apply(vector: Vector): Vector = {
    (std, mean) match {
      case (None, None) => throw new Error("Need to scaled with mean and/or with stdev") // TODO: better error
      case (Some(stdV), None) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length
            var i = 0
            while (i < size) {
              vs(i) *= (if (stdV(i) != 0.0) 1.0 / stdV(i) else 0.0)
              i += 1
            }
            Vectors.dense(vs)
          case SparseVector(size, indices, values) =>
            val vs = values.clone()
            val nnz = vs.length
            var i = 0
            while (i < nnz) {
              vs(i) *= (if (stdV(indices(i)) != 0.0) 1.0 / stdV(indices(i)) else 0.0)
              i += 1
            }
            Vectors.sparse(size, indices, vs)
        }
      case (None, Some(meanV)) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length
            var i = 0
            while(i < size) {
              vs(i) -= meanV(i)
              i += 1
            }
            Vectors.dense(vs)
        }
      case (Some(stdV), Some(meanV)) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length

            var i = 0
            while(i < size) {
              vs(i) = if(stdV(i) != 0.0) (vs(i) - meanV(i)) * (1.0 / stdV(i)) else 0.0
              i += 1
            }
            Vectors.dense(vs)
        }
    }
  }
}