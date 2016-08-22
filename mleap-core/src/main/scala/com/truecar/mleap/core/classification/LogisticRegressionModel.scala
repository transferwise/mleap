package com.truecar.mleap.core.classification

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS


/**
  * Binary Logistic Regression
  */
case class LogisticRegressionModel(coefficients: Vector,
                                   intercept: Double,
                                   threshold: Double = 0.5) extends ClassificationModel with Serializable {
  /** Computes the mean of the response variable given the predictors. */
  private def margin(features: Vector): Double = {
    BLAS.dot(features, coefficients) + intercept
  }

  /**
    * Computes the logit function over the given predictors, assuming a
    * binomial distribution.
    */
  private def score(features: Vector): Double = {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m)) // Compute the logit function over the given predictors.
  }

  def apply(features: Vector): Double = predict(features)

  override def predict(features: Vector): Double = {
    if (score(features) > threshold) 1.0 else 0.0
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }
}
