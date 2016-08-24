package com.truecar.mleap.runtime.serialization.bundle.ops.feature

import com.truecar.mleap.core.feature.ReverseStringIndexerModel
import com.truecar.mleap.runtime.transformer.feature.ReverseStringIndexer
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._

/**
  * Created by hollinwilkins on 8/24/16.
  */
object ReverseStringIndexerOp extends OpNode[ReverseStringIndexer, ReverseStringIndexerModel] {
  override val Model: OpModel[ReverseStringIndexerModel] = new OpModel[ReverseStringIndexerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(context: BundleContext, model: WritableModel, obj: ReverseStringIndexerModel): Unit = {
      model.withAttr(Attribute.stringList("labels", obj.labels))
    }

    override def load(context: BundleContext, model: ReadableModel): ReverseStringIndexerModel = {
      ReverseStringIndexerModel(labels = model.attr("labels").getStringList)
    }
  }

  override def name(node: ReverseStringIndexer): String = node.uid

  override def model(node: ReverseStringIndexer): ReverseStringIndexerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: ReverseStringIndexerModel): ReverseStringIndexer = {
    ReverseStringIndexer(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: ReverseStringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
