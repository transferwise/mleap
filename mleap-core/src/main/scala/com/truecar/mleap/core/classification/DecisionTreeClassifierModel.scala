package com.truecar.mleap.core.classification

import com.truecar.mleap.core.tree.Node
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class DecisionTreeClassifierModel(rootNode: Node,
                                       numFeatures: Int,
                                       numClasses: Int) {
  def apply(features: Vector): Double = {
    ProbabilisticClassification.rawToPrediction(predictRaw(features))
  }

  def predictRaw(features: Vector): Vector = {
    rootNode.predictImpl(features).impurities.get
  }
}
