package com.truecar.mleap.core.tree

import org.apache.spark.ml.linalg.Vector

/**
  * Created by hwilkins on 11/8/15.
  */
sealed trait Node extends Serializable {
  def predictImpl(features: Vector): LeafNode
}

final case class LeafNode(prediction: Double,
                          impurities: Option[Vector] = None) extends Node {
  override def predictImpl(features: Vector): LeafNode = this
}

final case class InternalNode(left: Node,
                              right: Node,
                              split: Split) extends Node {
  override def predictImpl(features: Vector): LeafNode = {
    if(split.shouldGoLeft(features)) {
      left.predictImpl(features)
    } else {
      right.predictImpl(features)
    }
  }
}
