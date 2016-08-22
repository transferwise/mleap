package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by hollinwilkins on 8/21/16.
  */
object HashingTermFrequencyOp extends OpNode[HashingTF, HashingTF] {
  override val Model: OpModel[HashingTF] = new OpModel[HashingTF] {
    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(context: BundleContext, model: WritableModel, obj: HashingTF): Unit = {
      model.withAttr(Attribute.long("num_features", obj.getNumFeatures)).
        withAttr(Attribute.boolean("binary", obj.getBinary))
    }

    override def load(context: BundleContext, model: ReadableModel): HashingTF = {
      new HashingTF(uid = "").setNumFeatures(model.attr("num_features").getLong.toInt).
        setBinary(model.attr("binary").getBoolean)
    }
  }

  override def name(node: HashingTF): String = node.uid

  override def model(node: HashingTF): HashingTF = node

  override def load(context: BundleContext, node: ReadableNode, model: HashingTF): HashingTF = {
    new HashingTF(uid = node.name).setNumFeatures(model.getNumFeatures).
      setBinary(model.getBinary).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: HashingTF): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
