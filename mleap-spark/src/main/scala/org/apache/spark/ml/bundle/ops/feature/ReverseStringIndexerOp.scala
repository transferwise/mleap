package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.feature.IndexToString

/**
  * Created by hollinwilkins on 8/21/16.
  */
object ReverseStringIndexerOp extends OpNode[IndexToString, IndexToString] {
  override val Model: OpModel[IndexToString] = new OpModel[IndexToString] {
    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(context: BundleContext, model: WritableModel, obj: IndexToString): Unit = {
      model.withAttr(Attribute.stringList("labels", obj.getLabels))
    }

    override def load(context: BundleContext, model: ReadableModel): IndexToString = {
      new IndexToString(uid = "").setLabels(model.attr("labels").getStringList.toArray)
    }
  }

  override def name(node: IndexToString): String = node.uid

  override def model(node: IndexToString): IndexToString = node

  override def load(context: BundleContext, node: ReadableNode, model: IndexToString): IndexToString = {
    new IndexToString(uid = node.name).copy(model.extractParamMap())
  }

  override def shape(node: IndexToString): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
