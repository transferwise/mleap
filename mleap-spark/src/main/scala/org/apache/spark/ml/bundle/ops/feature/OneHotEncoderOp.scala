package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.{AttributeBuilder, ShapeBuilder}
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object OneHotEncoderOp extends NodeOp[OneHotEncoderModel] {
  override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

  override def name(obj: OneHotEncoderModel): String = obj.uid

  override def shape(obj: OneHotEncoderModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: OneHotEncoderModel): Unit = {
    writer.withAttribute(ab.long("size", obj.size))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): OneHotEncoderModel = {
    new OneHotEncoderModel(node.name, node.attr("size").getLong.toInt).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }

}
