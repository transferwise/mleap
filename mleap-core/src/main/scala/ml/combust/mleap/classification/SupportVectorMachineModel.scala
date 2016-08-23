package ml.combust.mleap.classification

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

/**
  * Created by hollinwilkins on 4/14/16.
  */
object SupportVectorMachineModel {
  val defaultThreshold = 0.5
}

case class SupportVectorMachineModel(coefficients: Vector,
                                     intercept: Double,
                                     threshold: Double = SupportVectorMachineModel.defaultThreshold)
  extends ClassificationModel with Serializable {
  def apply(features: Vector): Double = predict(features)

  override def predict(features: Vector): Double = {
    if(margin(features) > threshold) 1.0 else 0.0
  }

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  private def margin(features: Vector): Double = BLAS.dot(coefficients, features) + intercept
}
