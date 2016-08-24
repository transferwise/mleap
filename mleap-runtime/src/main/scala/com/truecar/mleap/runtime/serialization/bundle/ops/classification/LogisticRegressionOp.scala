package com.truecar.mleap.runtime.serialization.bundle.ops.classification

import com.truecar.mleap.classification.LogisticRegressionModel
import com.truecar.mleap.runtime.transformer.classification.LogisticRegression
import ml.bundle.Bundle
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.wrapper.{Tensor, _}
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/24/16.
  */
object LogisticRegressionOp extends OpNode[LogisticRegression, LogisticRegressionModel] {
  override val Model: OpModel[LogisticRegressionModel] = new OpModel[LogisticRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.classification.logistic_regression

    override def store(context: BundleContext, model: WritableModel, obj: LogisticRegressionModel): Unit = {
      model.withAttr(Attribute.tensor("coefficients", Tensor.doubleVector(obj.coefficients.toArray.toSeq))).
        withAttr(Attribute.double("intercept", obj.intercept))
    }

    override def load(context: BundleContext, model: ReadableModel): LogisticRegressionModel = {
      LogisticRegressionModel(coefficients = Vectors.dense(model.attr("coefficients").getTensor.getDoubleVector.toArray),
        intercept = model.attr("intercept").getDouble)
    }
  }

  override def name(node: LogisticRegression): String = node.uid

  override def model(node: LogisticRegression): LogisticRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: LogisticRegressionModel): LogisticRegression = {
    LogisticRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: LogisticRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
