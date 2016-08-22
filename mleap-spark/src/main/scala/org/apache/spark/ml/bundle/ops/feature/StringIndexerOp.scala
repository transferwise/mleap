package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.{AttributeBuilder, ShapeBuilder}
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.StringIndexerModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object StringIndexerOp extends NodeOp[StringIndexerModel] {
  override def opName: String = Bundle.BuiltinOps.feature.string_indexer

  override def name(obj: StringIndexerModel): String = obj.uid

  override def shape(obj: StringIndexerModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: StringIndexerModel): Unit = {
    writer.withAttribute(ab.stringList("labels", obj.labels))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): StringIndexerModel = {
    new StringIndexerModel(uid = node.name,
      labels = node.attr("labels").getStringList.toArray).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }
}
