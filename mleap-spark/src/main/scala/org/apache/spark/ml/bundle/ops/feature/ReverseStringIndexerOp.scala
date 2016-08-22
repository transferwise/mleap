package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.IndexToString

/**
  * Created by hollinwilkins on 8/21/16.
  */
object ReverseStringIndexerOp extends NodeOp[IndexToString] {
  override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

  override def name(obj: IndexToString): String = obj.uid

  override def shape(obj: IndexToString, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: IndexToString): Unit = {
    writer.withAttribute(ab.stringList("labels", obj.getLabels))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): IndexToString = {
    new IndexToString(node.name).
      setLabels(node.attr("labels").getStringList.toArray).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }
}
