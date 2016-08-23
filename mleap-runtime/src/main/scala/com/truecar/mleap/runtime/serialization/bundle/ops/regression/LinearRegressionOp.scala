package com.truecar.mleap.runtime.serialization.bundle.ops.regression

import com.truecar.mleap.regression.LinearRegressionModel
import com.truecar.mleap.runtime.transformer.regression.LinearRegression
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object LinearRegressionOp extends OpNode[LinearRegression, LinearRegressionModel] {
  override val Model: OpModel[LinearRegressionModel] = new OpModel[LinearRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.linear_regression

    override def store(context: BundleContext, model: WritableModel, obj: LinearRegressionModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.coefficients.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.intercept))
    }

    override def load(context: BundleContext, model: ReadableModel): LinearRegressionModel = {
      LinearRegressionModel(coefficients = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)
    }
  }

  override def name(node: LinearRegression): String = node.uid

  override def model(node: LinearRegression): LinearRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: LinearRegressionModel): LinearRegression = {
    LinearRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: LinearRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
