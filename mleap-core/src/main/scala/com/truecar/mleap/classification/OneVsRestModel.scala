package com.truecar.mleap.classification

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 8/22/16.
  */
case class OneVsRestModel(classifiers: Array[ClassificationModel]) {
  def apply(features: Vector): Double = predict(features)

  def predict(features: Vector): Double = {
    classifiers.zipWithIndex.map {
      case (c, i) => (c.predictRaw(features)(1), i)
    }.maxBy(_._1)._2
  }
}
