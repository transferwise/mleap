package org.apache.spark.ml.bundle.ops.regression

import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object LinearRegressionOp extends OpNode[LinearRegressionModel, LinearRegressionModel] {
  override val Model: OpModel[LinearRegressionModel] = new OpModel[LinearRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(context: BundleContext, model: WritableModel, obj: LinearRegressionModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.coefficients.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.intercept))
    }

    override def load(context: BundleContext, model: ReadableModel): LinearRegressionModel = {
      new LinearRegressionModel(uid = "",
        coefficients = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)
    }
  }

  override def name(node: LinearRegressionModel): String = node.uid

  override def model(node: LinearRegressionModel): LinearRegressionModel = node

  override def load(context: BundleContext, node: ReadableNode, model: LinearRegressionModel): LinearRegressionModel = {
    new LinearRegressionModel(uid = node.name,
      coefficients = model.coefficients,
      intercept = model.intercept).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: LinearRegressionModel): Shape = Shape().
    withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
