package com.truecar.mleap.regression

import org.apache.spark.ml.linalg.Vector
import com.truecar.mleap.tree.{DecisionTree, Node}

/**
 * Created by hwilkins on 11/8/15.
 */
case class DecisionTreeRegressionModel(rootNode: Node, numFeatures: Int) extends DecisionTree {
  def apply(features: Vector): Double = predict(features)

  def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }
}
