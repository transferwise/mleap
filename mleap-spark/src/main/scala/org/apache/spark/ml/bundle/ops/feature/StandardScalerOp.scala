package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.ShapeBuilder
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object StandardScalerOp extends NodeOp[StandardScalerModel] {
  override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

  override def name(obj: StandardScalerModel): String = obj.uid

  override def shape(obj: StandardScalerModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withStandardIO(obj.getInputCol, obj.getOutputCol).build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: StandardScalerModel): Unit = {
    if(obj.getWithMean) {
      writer.withAttribute(ab.tensor("mean", tb.doubleVector(obj.mean.toArray)))
    }

    if(obj.getWithStd) {
      writer.withAttribute(ab.tensor("std", tb.doubleVector(obj.std.toArray)))
    }
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): StandardScalerModel = {
    val mean = node.getAttr("mean").map(t => Vectors.dense(t.getTensor.doubleVal.toArray)).orNull
    val std = node.getAttr("std").map(t => Vectors.dense(t.getTensor.doubleVal.toArray)).orNull
    new StandardScalerModel(uid = node.name, mean = mean, std = std).
      setInputCol(node.standardInput.name).
      setOutputCol(node.standardOutput.name)
  }

}
