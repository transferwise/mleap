package com.truecar.mleap.runtime.bundle

import com.truecar.mleap.runtime.transformer.{PipelineModel, Transformer}
import ml.bundle.wrapper.Attribute
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, GraphSerializer}
import ml.bundle.wrapper.{ReadableModel, ReadableNode, Shape, WritableModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object PipelineOp extends OpNode[PipelineModel, PipelineModel] {
  override val Model: OpModel[PipelineModel] = new OpModel[PipelineModel] {
    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(context: BundleContext, model: WritableModel, obj: PipelineModel): Unit = {
      val nodes = GraphSerializer(context).write(obj.transformers)
      model.withAttr(Attribute.stringList("nodes", nodes))
    }

    override def load(context: BundleContext, model: ReadableModel): PipelineModel = {
      val nodes = GraphSerializer(context).read(model.attr("nodes").getStringList).map(_.asInstanceOf[Transformer])
      PipelineModel(transformers = nodes)
    }
  }

  override def name(node: PipelineModel): String = node.uid

  override def model(node: PipelineModel): PipelineModel = node

  override def load(context: BundleContext, node: ReadableNode, model: PipelineModel): PipelineModel = {
    model.copy(uid = node.name)
  }

  override def shape(node: PipelineModel): Shape = Shape()
}
