package com.truecar.mleap.classification

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait ClassificationModel {
  def predict(features: Vector): Double = {
    raw2prediction(predictRaw(features))
  }

  def predictRaw(features: Vector): Vector

  protected def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax
}
