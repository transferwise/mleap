package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.Tokenizer

/**
  * Created by hollinwilkins on 8/21/16.
  */
object TokenizerOp extends NodeOp[Tokenizer] {
  override def opName: String = Bundle.BuiltinOps.feature.tokenizer

  override def name(obj: Tokenizer): String = obj.uid

  override def shape(obj: Tokenizer, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: Tokenizer): Unit = { }

  override def read(reader: NodeReader, node: NodeDefWrapper): Tokenizer = {
    new Tokenizer(uid = node.name).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }

}
