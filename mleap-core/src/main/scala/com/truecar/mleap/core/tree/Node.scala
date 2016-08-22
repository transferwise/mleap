package com.truecar.mleap.core.tree

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hwilkins on 11/8/15.
  */
sealed trait Node extends Serializable {
  def prediction: Double

  def predictImpl(features: Vector): LeafNode
}

final case class LeafNode(prediction: Double,
                          impurityStats: Option[Vector] = None) extends Node {
  override def predictImpl(features: Vector): LeafNode = this
}

final case class InternalNode(prediction: Double,
                              impurity: Double,
                              gain: Double,
                              leftChild: Node,
                              rightChild: Node,
                              split: Split) extends Node {
  override def predictImpl(features: Vector): LeafNode = {
    if(split.shouldGoLeft(features)) {
      leftChild.predictImpl(features)
    } else {
      rightChild.predictImpl(features)
    }
  }
}
