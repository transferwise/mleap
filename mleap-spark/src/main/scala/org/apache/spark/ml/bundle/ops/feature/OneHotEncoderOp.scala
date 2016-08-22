package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object OneHotEncoderOp extends OpNode[OneHotEncoderModel, OneHotEncoderModel] {
  override val Model: OpModel[OneHotEncoderModel] = new OpModel[OneHotEncoderModel] {
    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(context: BundleContext, model: WritableModel, obj: OneHotEncoderModel): Unit = {
      model.withAttr(Attribute.long("size", obj.size))
    }

    override def load(context: BundleContext, model: ReadableModel): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", size = model.attr("size").getLong.toInt)
    }
  }

  override def name(node: OneHotEncoderModel): String = node.uid

  override def model(node: OneHotEncoderModel): OneHotEncoderModel = node

  override def load(context: BundleContext, node: ReadableNode, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = node.name, size = model.size)
  }

  override def shape(node: OneHotEncoderModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
