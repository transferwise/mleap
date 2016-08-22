package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.{AttributeBuilder, ShapeBuilder}
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by hollinwilkins on 8/21/16.
  */
object HashingTermFrequencyOp extends NodeOp[HashingTF] {
  override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

  override def name(obj: HashingTF): String = obj.uid

  override def shape(obj: HashingTF, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: HashingTF): Unit = {
    writer.withAttribute(ab.long("num_features", obj.getNumFeatures)).
      withAttribute(ab.boolean("binary", obj.getBinary))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): HashingTF = {
    new HashingTF(node.name).
      setNumFeatures(node.attr("num_features").getLong.toInt).
      setBinary(node.attr("binary").getBoolean).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }
}
