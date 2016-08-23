package ml.combust.mleap.runtime.bundle.tree

import ml.combust.mleap.tree
import ml.bundle.tree.Split.Split
import ml.bundle.tree.Split.Split.{CategoricalSplit, ContinuousSplit}
import ml.bundle.tree.Node.Node
import ml.bundle.tree.Node.Node.{InternalNode, LeafNode}
import ml.bundle.tree.NodeWrapper
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapNodeWrapper extends NodeWrapper[tree.Node] {
  override def node(node: tree.Node, withImpurities: Boolean): Node = node match {
    case node: tree.InternalNode =>
      val split = node.split match {
        case split: tree.CategoricalSplit =>
          Split(Split.S.Categorical(CategoricalSplit(featureIndex = split.featureIndex,
            isLeft = split.isLeft,
            numCategories = split.numCategories,
            categories = split.categories)))
        case split: tree.ContinuousSplit =>
          Split(Split.S.Continuous(ContinuousSplit(featureIndex = split.featureIndex,
            threshold = split.threshold)))
      }
      Node(Node.N.Internal(Node.InternalNode(split)))
    case node: tree.LeafNode =>
      val impurities = if(withImpurities) {
        node.impurities.get.toArray.toSeq
      } else { Seq() }
      Node(Node.N.Leaf(Node.LeafNode(node.prediction, impurities)))
  }

  override def isInternal(node: tree.Node): Boolean = node.isInstanceOf[tree.InternalNode]

  override def leaf(node: LeafNode, withImpurities: Boolean): tree.Node = {
    val impurities = if(withImpurities) {
      Some(Vectors.dense(node.impurities.toArray))
    } else {
      None
    }

    tree.LeafNode(prediction = node.prediction,
      impurities = impurities)
  }

  override def internal(node: InternalNode,
                        left: tree.Node,
                        right: tree.Node): tree.Node = {
    val split = if(node.split.s.isCategorical) {
      val s = node.split.getCategorical
      tree.CategoricalSplit(featureIndex = s.featureIndex,
        isLeft = s.isLeft,
        numCategories = s.numCategories,
        categories = s.categories.toArray)
    } else if(node.split.s.isContinuous) {
      val s = node.split.getContinuous
      tree.ContinuousSplit(featureIndex = s.featureIndex,
        threshold = s.threshold)
    } else { throw new Error("invalid split") }

    tree.InternalNode(split = split,
      left = left,
      right = right)
  }

  override def left(node: tree.Node): tree.Node = node match {
    case node: tree.InternalNode => node.left
    case _ => throw new Error("not an internal node") // TODO: better error
  }

  override def right(node: tree.Node): tree.Node = node match {
    case node: tree.InternalNode => node.right
    case _ => throw new Error("not an internal node") // TODO: better error
  }
}
