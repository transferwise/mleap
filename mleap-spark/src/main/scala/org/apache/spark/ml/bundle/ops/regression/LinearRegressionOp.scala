package org.apache.spark.ml.bundle.ops.regression

import ml.bundle.Bundle
import ml.bundle.Shape.Shape
import ml.bundle.builder.{AttributeBuilder, ShapeBuilder, TensorBuilder}
import ml.bundle.serializer.{AttributeWriter, NodeOp, NodeReader, OpRegistry}
import ml.bundle.util.NodeDefWrapper
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LinearRegressionOp extends NodeOp[LinearRegressionModel] {
  override def opName: String = Bundle.BuiltinOps.regression.linear_regression

  override def name(obj: LinearRegressionModel): String = obj.uid

  override def shape(obj: LinearRegressionModel, registry: OpRegistry): Shape = {
    ShapeBuilder().withInput(obj.getFeaturesCol, "features").
      withOutput(obj.getPredictionCol, "prediction").
      build()
  }

  override def writeAttributes(writer: AttributeWriter, obj: LinearRegressionModel): Unit = {
    writer.withAttribute(ab.tensor("coefficients", tb.doubleVector(obj.coefficients.toArray))).
      withAttribute(ab.double("intercept", obj.intercept))
  }

  override def read(reader: NodeReader, node: NodeDefWrapper): LinearRegressionModel = {
    new LinearRegressionModel(uid = node.name,
      coefficients = Vectors.dense(node.attr("coefficients").getTensor.doubleVal.toArray),
      intercept = node.attr("intercept").getDouble).
      setFeaturesCol(node.input("features").name).
      setPredictionCol(node.output("prediction").name)
  }
}
