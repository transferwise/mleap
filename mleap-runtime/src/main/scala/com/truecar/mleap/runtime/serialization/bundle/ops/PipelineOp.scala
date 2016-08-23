package com.truecar.mleap.runtime.serialization.bundle.ops

import com.truecar.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, GraphSerializer}
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object PipelineOp extends OpNode[Pipeline, Pipeline] {
  override val Model: OpModel[Pipeline] = new OpModel[Pipeline] {
    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(context: BundleContext, model: WritableModel, obj: Pipeline): Unit = {
      val nodes = GraphSerializer(context).write(obj.transformers)
      model.withAttr(Attribute.stringList("nodes", nodes))
    }

    override def load(context: BundleContext, model: ReadableModel): Pipeline = {
      val nodes = GraphSerializer(context).read(model.attr("nodes").getStringList).map(_.asInstanceOf[Transformer])
      Pipeline(transformers = nodes)
    }
  }

  override def name(node: Pipeline): String = node.uid

  override def model(node: Pipeline): Pipeline = node

  override def load(context: BundleContext, node: ReadableNode, model: Pipeline): Pipeline = {
    model.copy(uid = node.name)
  }

  override def shape(node: Pipeline): Shape = Shape()
}
