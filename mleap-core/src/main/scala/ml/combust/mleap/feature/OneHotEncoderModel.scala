package ml.combust.mleap.feature

import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
 * Created by hwilkins on 11/5/15.
 */
case class OneHotEncoderModel(size: Int) extends Serializable {
  val oneValue = Array(1.0)
  val emptyIndices = Array[Int]()
  val emptyValues = Array[Double]()

  def apply(label: Double): Vector = {
    val labelInt = label.toInt

    if(label != labelInt) {
      throw new Error("invalid label, must be integer")
    }

    if(label < size) {
      Vectors.sparse(size, Array(labelInt), oneValue)
    } else {
      Vectors.sparse(size, emptyIndices, emptyValues)
    }
  }
}
